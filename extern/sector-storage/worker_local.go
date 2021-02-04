package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-sysinfo"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	storage "github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

var pathTypes = []storiface.SectorFileType{storiface.FTUnsealed, storiface.FTSealed, storiface.FTCache}

type WorkerConfig struct {
	TaskTypes          []sealtasks.TaskType
	NoSwap             bool
	PreCommit1Max      uint64
	PreCommit2Max      uint64
	Commit2Max         uint64
	DiskHoldMax        uint64
	IsMiner            bool
	ForceP2FromLocalP1 bool
	AllowMyScheduler   bool
}

// used do provide custom proofs impl (mostly used in testing)
type ExecutorFunc func() (ffiwrapper.Storage, error)

type LocalWorker struct {
	storage    stores.Store
	localStore *stores.Local
	sindex     stores.SectorIndex
	ret        storiface.WorkerReturn
	executor   ExecutorFunc
	noSwap     bool

	ct          *workerCallTracker
	acceptTasks map[sealtasks.TaskType]struct{}
	running     sync.WaitGroup
	taskLk      sync.Mutex

	session     uuid.UUID
	testDisable int64
	closing     chan struct{}

	isMiner            bool
	preCommit1Max      uint64
	preCommit2Max      uint64
	commit2Max         uint64
	diskHoldMax        uint64
	allowMyScheduler   bool
	forceP2FromLocalP1 bool
}

func newLocalWorker(executor ExecutorFunc, wcfg WorkerConfig, store stores.Store, local *stores.Local, sindex stores.SectorIndex, ret storiface.WorkerReturn, cst *statestore.StateStore) *LocalWorker {
	acceptTasks := map[sealtasks.TaskType]struct{}{}
	for _, taskType := range wcfg.TaskTypes {
		acceptTasks[taskType] = struct{}{}
	}

	w := &LocalWorker{
		storage:    store,
		localStore: local,
		sindex:     sindex,
		ret:        ret,

		ct: &workerCallTracker{
			st: cst,
		},
		acceptTasks: acceptTasks,
		executor:    executor,
		noSwap:      wcfg.NoSwap,

		session: uuid.New(),
		closing: make(chan struct{}),
		preCommit1Max:      wcfg.PreCommit1Max,
		preCommit2Max:      wcfg.PreCommit2Max,
		commit2Max:         wcfg.Commit2Max,
		diskHoldMax:        wcfg.DiskHoldMax,
		isMiner:            wcfg.IsMiner,
		allowMyScheduler:   wcfg.AllowMyScheduler,
		forceP2FromLocalP1: wcfg.ForceP2FromLocalP1,
	}

	if w.executor == nil {
		w.executor = w.ffiExec
	}

	unfinished, err := w.ct.unfinished()
	if err != nil {
		log.Errorf("reading unfinished tasks: %+v", err)
		return w
	}

	go func() {
		for _, call := range unfinished {
			err := storiface.Err(storiface.ErrTempWorkerRestart, xerrors.New("worker restarted"))

			// TODO: Handle restarting PC1 once support is merged

			if doReturn(context.TODO(), call.RetType, call.ID, ret, nil, err) {
				if err := w.ct.onReturned(call.ID); err != nil {
					log.Errorf("marking call as returned failed: %s: %+v", call.RetType, err)
				}
			}
		}
	}()

	return w
}

func NewLocalWorker(wcfg WorkerConfig, store stores.Store, local *stores.Local, sindex stores.SectorIndex, ret storiface.WorkerReturn, cst *statestore.StateStore) *LocalWorker {
	return newLocalWorker(nil, wcfg, store, local, sindex, ret, cst)
}

type localWorkerPathProvider struct {
	w  *LocalWorker
	op storiface.AcquireMode
}

func (l *localWorkerPathProvider) AcquireSector(ctx context.Context, sector storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, sealing storiface.PathType) (storiface.SectorPaths, func(), error) {
	paths, storageIDs, err := l.w.storage.AcquireSector(ctx, sector, existing, allocate, sealing, l.op)
	if err != nil {
		return storiface.SectorPaths{}, nil, err
	}

	releaseStorage, err := l.w.localStore.Reserve(ctx, sector, allocate, storageIDs, storiface.FSOverheadSeal)
	if err != nil {
		return storiface.SectorPaths{}, nil, xerrors.Errorf("reserving storage space: %w", err)
	}

	log.Debugf("acquired sector %d (e:%d; a:%d): %v", sector, existing, allocate, paths)

	return paths, func() {
		releaseStorage()

		for _, fileType := range pathTypes {
			if fileType&allocate == 0 {
				continue
			}

			sid := storiface.PathByType(storageIDs, fileType)

			if err := l.w.sindex.StorageDeclareSector(ctx, stores.ID(sid), sector.ID, fileType, l.op == storiface.AcquireMove); err != nil {
				log.Errorf("declare sector error: %+v", err)
			}
		}
	}, nil
}

func (l *LocalWorker) ffiExec() (ffiwrapper.Storage, error) {
	return ffiwrapper.New(&localWorkerPathProvider{w: l})
}

type ReturnType string

const (
	AddPiece        ReturnType = "AddPiece"
	SealPreCommit1  ReturnType = "SealPreCommit1"
	SealPreCommit2  ReturnType = "SealPreCommit2"
	SealCommit1     ReturnType = "SealCommit1"
	SealCommit2     ReturnType = "SealCommit2"
	FinalizeSector  ReturnType = "FinalizeSector"
	ReleaseUnsealed ReturnType = "ReleaseUnsealed"
	MoveStorage     ReturnType = "MoveStorage"
	UnsealPiece     ReturnType = "UnsealPiece"
	ReadPiece       ReturnType = "ReadPiece"
	Fetch           ReturnType = "Fetch"
)

// in: func(WorkerReturn, context.Context, CallID, err string)
// in: func(WorkerReturn, context.Context, CallID, ret T, err string)
func rfunc(in interface{}) func(context.Context, storiface.CallID, storiface.WorkerReturn, interface{}, *storiface.CallError) error {
	rf := reflect.ValueOf(in)
	ft := rf.Type()
	withRet := ft.NumIn() == 5

	return func(ctx context.Context, ci storiface.CallID, wr storiface.WorkerReturn, i interface{}, err *storiface.CallError) error {
		rctx := reflect.ValueOf(ctx)
		rwr := reflect.ValueOf(wr)
		rerr := reflect.ValueOf(err)
		rci := reflect.ValueOf(ci)

		var ro []reflect.Value

		if withRet {
			ret := reflect.ValueOf(i)
			if i == nil {
				ret = reflect.Zero(rf.Type().In(3))
			}

			ro = rf.Call([]reflect.Value{rwr, rctx, rci, ret, rerr})
		} else {
			ro = rf.Call([]reflect.Value{rwr, rctx, rci, rerr})
		}

		if !ro[0].IsNil() {
			return ro[0].Interface().(error)
		}

		return nil
	}
}

var returnFunc = map[ReturnType]func(context.Context, storiface.CallID, storiface.WorkerReturn, interface{}, *storiface.CallError) error{
	AddPiece:        rfunc(storiface.WorkerReturn.ReturnAddPiece),
	SealPreCommit1:  rfunc(storiface.WorkerReturn.ReturnSealPreCommit1),
	SealPreCommit2:  rfunc(storiface.WorkerReturn.ReturnSealPreCommit2),
	SealCommit1:     rfunc(storiface.WorkerReturn.ReturnSealCommit1),
	SealCommit2:     rfunc(storiface.WorkerReturn.ReturnSealCommit2),
	FinalizeSector:  rfunc(storiface.WorkerReturn.ReturnFinalizeSector),
	ReleaseUnsealed: rfunc(storiface.WorkerReturn.ReturnReleaseUnsealed),
	MoveStorage:     rfunc(storiface.WorkerReturn.ReturnMoveStorage),
	UnsealPiece:     rfunc(storiface.WorkerReturn.ReturnUnsealPiece),
	ReadPiece:       rfunc(storiface.WorkerReturn.ReturnReadPiece),
	Fetch:           rfunc(storiface.WorkerReturn.ReturnFetch),
}

func (l *LocalWorker) asyncCall(ctx context.Context, sector storage.SectorRef, rt ReturnType, work func(ctx context.Context, ci storiface.CallID) (interface{}, error)) (storiface.CallID, error) {
	ci := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}

	if err := l.ct.onStart(ci, rt); err != nil {
		log.Errorf("tracking call (start): %+v", err)
	}

	l.running.Add(1)

	go func() {
		defer l.running.Done()

		ctx := &wctx{
			vals:    ctx,
			closing: l.closing,
		}

		res, err := work(ctx, ci)

		if err != nil {
			rb, err := json.Marshal(res)
			if err != nil {
				log.Errorf("tracking call (marshaling results): %+v", err)
			} else {
				if err := l.ct.onDone(ci, rb); err != nil {
					log.Errorf("tracking call (done): %+v", err)
				}
			}
		}

		if doReturn(ctx, rt, ci, l.ret, res, toCallError(err)) {
			if err := l.ct.onReturned(ci); err != nil {
				log.Errorf("tracking call (done): %+v", err)
			}
		}
	}()

	return ci, nil
}

func toCallError(err error) *storiface.CallError {
	var serr *storiface.CallError
	if err != nil && !xerrors.As(err, &serr) {
		serr = storiface.Err(storiface.ErrUnknown, err)
	}

	return serr
}

// doReturn tries to send the result to manager, returns true if successful
func doReturn(ctx context.Context, rt ReturnType, ci storiface.CallID, ret storiface.WorkerReturn, res interface{}, rerr *storiface.CallError) bool {
	for {
		err := returnFunc[rt](ctx, ci, ret, res, rerr)
		if err == nil {
			break
		}

		log.Errorf("return error, will retry in 5s: %s: %+v", rt, err)
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			log.Errorf("failed to return results: %s", ctx.Err())

			// fine to just return, worker is most likely shutting down, and
			// we didn't mark the result as returned yet, so we'll try to
			// re-submit it on restart
			return false
		}
	}

	return true
}

func (l *LocalWorker) NewSector(ctx context.Context, sector storage.SectorRef) error {
	sb, err := l.executor()
	if err != nil {
		return err
	}

	return sb.NewSector(ctx, sector)
}

func (l *LocalWorker) AddPiece(ctx context.Context, sector storage.SectorRef, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader, isFromDeal bool) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, AddPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
	        //return sb.AddPiece(ctx, sector, epcs, sz, r)
		// --------------- Copy add piece start ---------------
		//time.Sleep(30 * time.Second)
		//if !l.isMiner {
		//	return sb.AddPiece(ctx, sector, epcs, sz, r)
		//}
		if isFromDeal {
			return sb.AddPiece(ctx, sector, epcs, sz, r)
		}
		sp := ctx.Value(SchedPriorityKey)
		if _, ok := sp.(int); ok {
			return sb.AddPiece(ctx, sector, epcs, sz, r)
		}
		if len(epcs) > 0 {
			return sb.AddPiece(ctx, sector, epcs, sz, r)
		}
		var p abi.PieceInfo
		var out []stores.StoragePath
		if out, err = l.Paths(ctx); err != nil {
			log.Errorf("error: %+v", err)
			return sb.AddPiece(ctx, sector, epcs, sz, r)
		}
		for _, path := range out {
			if !path.CanSeal {
				continue
			}
			var apPieceCID = filepath.Join(path.LocalPath, fmt.Sprintf("addpieceCID_%d", sector.ID.Miner))
			var addPieceFile = filepath.Join(path.LocalPath, fmt.Sprintf("addpiece_%d", sector.ID.Miner))
			_, error := os.Stat(addPieceFile)
			if os.IsNotExist(error) {
				p, err = sb.AddPiece(ctx, sector, epcs, sz, r)
				//l.initAddPiece = true
				//l.initPieceInfo = p
				fileSrc := filepath.Join(path.LocalPath, fmt.Sprintf("unsealed/s-t0%d-%d", sector.ID.Miner, sector.ID.Number))
				fileDst := addPieceFile
				_, error := os.Stat(fileSrc)
				if !os.IsNotExist(error) {
					//l.initAddPieceFile = fmt.Sprintf("addpiece_%d", sector.ID.Miner)
					_, error := os.Stat(fileDst)
					if !os.IsNotExist(error) {
						os.Remove(fileDst)
					}
					input, error := ioutil.ReadFile(fileSrc)
					if error == nil {
						error = ioutil.WriteFile(fileDst, input, 0755)
					}
				}
				var str = fmt.Sprintf("%+v", p.PieceCID)
				b := []byte(str)
				_ = ioutil.WriteFile(apPieceCID, b, 0755)
				return p, err
			}

			apPieceCIDtxt, error := ioutil.ReadFile(apPieceCID)
			if os.IsNotExist(error) {
				return sb.AddPiece(ctx, sector, epcs, sz, r)
			}
			var dummyCid, errorCID = cid.Parse(string(apPieceCIDtxt))
			if errorCID != nil {
				log.Debugf("Copy add piece error s-t0%d-%d, cid.Parse(string(%s) ) from %s ", sector.ID.Miner, sector.ID.Number, string(apPieceCIDtxt), apPieceCID)
				return sb.AddPiece(ctx, sector, epcs, sz, r)
			}
			ssize, err := sector.ProofType.SectorSize()
			if err != nil {
				ssize = abi.SectorSize(34359738368)
			}
			p = abi.PieceInfo{
				Size:     abi.PaddedPieceSize(ssize),
				PieceCID: dummyCid,
			}
			fileSrc := addPieceFile
			fileDst := filepath.Join(path.LocalPath, fmt.Sprintf("unsealed/s-t0%d-%d", sector.ID.Miner, sector.ID.Number))
			_, error = os.Stat(fileDst)
			if !os.IsNotExist(error) {
				os.Remove(fileDst)
			}
			input, error := ioutil.ReadFile(fileSrc)
			if error != nil {
				return sb.AddPiece(ctx, sector, epcs, sz, r)
			}
			error = ioutil.WriteFile(fileDst, input, 0755)
			if error != nil {
				return sb.AddPiece(ctx, sector, epcs, sz, r)
			}
			if err := l.sindex.StorageDeclareSector(ctx, path.ID, sector.ID, storiface.FTUnsealed, path.CanStore); err != nil {
				//return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", sector, stores.FTUnsealed, path.ID, err)
				return sb.AddPiece(ctx, sector, epcs, sz, r)
			}
			log.Debugf("Copy add piece for sector %d successfully to %s", sector.ID.Number, fileDst)
			return p, err
		}
		// --------------- Copy add piece end ---------------
		return sb.AddPiece(ctx, sector, epcs, sz, r)
	})
}

func (l *LocalWorker) Fetch(ctx context.Context, sector storage.SectorRef, fileType storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, Fetch, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		_, done, err := (&localWorkerPathProvider{w: l, op: am}).AcquireSector(ctx, sector, fileType, storiface.FTNone, ptype)
		if err == nil {
			done()
		}

		return nil, err
	})
}

func (l *LocalWorker) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, SealPreCommit1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {

		{
			// cleanup previous failed attempts if they exist
			if err := l.storage.Remove(ctx, sector.ID, storiface.FTSealed, true); err != nil {
				return nil, xerrors.Errorf("cleaning up sealed data: %w", err)
			}

			if err := l.storage.Remove(ctx, sector.ID, storiface.FTCache, true); err != nil {
				return nil, xerrors.Errorf("cleaning up cache data: %w", err)
			}
		}

		sb, err := l.executor()
		if err != nil {
			return nil, err
		}

		return sb.SealPreCommit1(ctx, sector, ticket, pieces)
	})
}

func (l *LocalWorker) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealPreCommit2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.SealPreCommit2(ctx, sector, phase1Out)
	})
}

func (l *LocalWorker) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealCommit1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
	})
}

func (l *LocalWorker) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealCommit2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.SealCommit2(ctx, sector, phase1Out)
	})
}

func (l *LocalWorker) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, FinalizeSector, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		if err := sb.FinalizeSector(ctx, sector, keepUnsealed); err != nil {
			return nil, xerrors.Errorf("finalizing sector: %w", err)
		}

		if len(keepUnsealed) == 0 {
			if err := l.storage.Remove(ctx, sector.ID, storiface.FTUnsealed, true); err != nil {
				return nil, xerrors.Errorf("removing unsealed data: %w", err)
			}
		}

		return nil, err
	})
}

func (l *LocalWorker) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (storiface.CallID, error) {
	return storiface.UndefCall, xerrors.Errorf("implement me")
}

func (l *LocalWorker) Remove(ctx context.Context, sector abi.SectorID) error {
	var err error

	if rerr := l.storage.Remove(ctx, sector, storiface.FTSealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTCache, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTUnsealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (l *LocalWorker) MoveStorage(ctx context.Context, sector storage.SectorRef, types storiface.SectorFileType) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, MoveStorage, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return nil, l.storage.MoveStorage(ctx, sector, types)
	})
}

func (l *LocalWorker) UnsealPiece(ctx context.Context, sector storage.SectorRef, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, cid cid.Cid) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, UnsealPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		if err = sb.UnsealPiece(ctx, sector, index, size, randomness, cid); err != nil {
			return nil, xerrors.Errorf("unsealing sector: %w", err)
		}

		if err = l.storage.RemoveCopies(ctx, sector.ID, storiface.FTSealed); err != nil {
			return nil, xerrors.Errorf("removing source data: %w", err)
		}

		if err = l.storage.RemoveCopies(ctx, sector.ID, storiface.FTCache); err != nil {
			return nil, xerrors.Errorf("removing source data: %w", err)
		}

		return nil, nil
	})
}

func (l *LocalWorker) ReadPiece(ctx context.Context, writer io.Writer, sector storage.SectorRef, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, ReadPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.ReadPiece(ctx, writer, sector, index, size)
	})
}

func (l *LocalWorker) TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error) {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	return l.acceptTasks, nil
}

func (l *LocalWorker) TaskDisable(ctx context.Context, tt sealtasks.TaskType) error {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	delete(l.acceptTasks, tt)
	return nil
}

func (l *LocalWorker) TaskEnable(ctx context.Context, tt sealtasks.TaskType) error {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	l.acceptTasks[tt] = struct{}{}
	return nil
}

func (l *LocalWorker) Paths(ctx context.Context) ([]stores.StoragePath, error) {
	return l.localStore.Local(ctx)
}

func (l *LocalWorker) localDiskUsed() ( uint64, uint64) {
	var cacheCount = uint64(0)
	var apCount = uint64(0)
	var out []stores.StoragePath
	out, err := l.Paths(context.TODO())
	if err != nil {
		log.Error(err)
		return cacheCount, apCount
	}
	for _, path := range out {
		if path.CanStore {
			continue
		}
		ents, err := ioutil.ReadDir(filepath.Join(path.LocalPath, "cache"))
		if !os.IsNotExist(err) {
			for _, ent := range ents {
				if ent.Name() == "fetching" {
					continue
				}
				fileDst := filepath.Join(path.LocalPath, fmt.Sprintf("cache/%s/sc-02-data-layer-1.dat", ent.Name()))
				_, error := os.Stat(fileDst)
				if !os.IsNotExist(error) {
					cacheCount = cacheCount + 14
				}
			}
		}

		ents, err = ioutil.ReadDir(filepath.Join(path.LocalPath, "unsealed"))
		if !os.IsNotExist(err) {
			for _, ent := range ents {
				if ent.Name() == "fetching" {
					continue
				}
				cacheCount++
				apCount++
			}
		}

		ents, err = ioutil.ReadDir(filepath.Join(path.LocalPath, "sealed"))
		if !os.IsNotExist(err) {
			for _, ent := range ents {
				if ent.Name() == "fetching" {
					continue
				}
				cacheCount++
			}
		}
	}
	return cacheCount, apCount
}

func (l *LocalWorker) Info(context.Context) (storiface.WorkerInfo, error) {
	hostname, err := os.Hostname() // TODO: allow overriding from config
	if err != nil {
		hostname = "" //panic(err)
	}
	//offline, ok := os.LookupEnv("FIL_I_WANT_OFFLINE")
	//if !ok {
	//	offline = "0"
	//}
	var addPieceMax = uint64(3)
	var preCommit1Max = l.preCommit1Max
	var preCommit2Max = l.preCommit2Max
	var commit2Max = l.commit2Max
	var diskHoldMax = l.diskHoldMax
	var apDiskHoldMax = uint64(0)
	var forceP2FromLocalP1 = l.forceP2FromLocalP1
	var forceP1FromLocalAP = false
	var forceC2FromLocalP2 = false
	var isPlanOffline = false
	var allowP2C2Parallel = false
	var autoPledgeDiff = uint64(0)
	var ignoreOutOfSpace = false
	var out []stores.StoragePath
	if out, err = l.Paths(context.TODO()); err == nil {
		for _, path := range out {
			//log.Debugf("path.LocalPath: %s on worker %d for s-t0%d-%d", path.LocalPath, wid, sector.Miner, sector.Number)
			mb, err := ioutil.ReadFile(filepath.Join(path.LocalPath, "myscheduler.json"))
			if err == nil {
				var meta stores.MySchedulerMeta
				if err := json.Unmarshal(mb, &meta); err == nil {
					addPieceMax = meta.AddPieceMax
					preCommit1Max = meta.PreCommit1Max
					preCommit2Max = meta.PreCommit2Max
					commit2Max = meta.Commit2Max
					diskHoldMax = meta.DiskHoldMax
					apDiskHoldMax = meta.APDiskHoldMax
					forceP1FromLocalAP = meta.ForceP1FromLocalAP
					forceP2FromLocalP1 = meta.ForceP2FromLocalP1
					forceC2FromLocalP2 = meta.ForceC2FromLocalP2
					isPlanOffline = meta.IsPlanOffline
					allowP2C2Parallel = meta.AllowP2C2Parallel
					autoPledgeDiff = meta.AutoPledgeDiff
					ignoreOutOfSpace = meta.IgnoreOutOfSpace
					if (meta.WorkerName != "") {
						hostname = meta.WorkerName
					}
				}
			}
		}
	}

	//l.taskLk.Lock()
	//defer l.taskLk.Unlock()
	//if addPieceMax > 0 {
	//	l.acceptTasks[sealtasks.TTAddPiece] = struct{}{}
	//}else{
	//	delete(l.acceptTasks, sealtasks.TTAddPiece)
	//}
	//if preCommit1Max > 0 {
	//	l.acceptTasks[sealtasks.TTPreCommit1] = struct{}{}
	//}else{
	//	delete(l.acceptTasks, sealtasks.TTPreCommit1)
	//}
	//if preCommit2Max > 0 {
	//	l.acceptTasks[sealtasks.TTPreCommit2] = struct{}{}
	//}else{
	//	delete(l.acceptTasks, sealtasks.TTPreCommit2)
	//}
	//if commit2Max > 0 {
	//	l.acceptTasks[sealtasks.TTCommit2] = struct{}{}
	//}else{
	//	delete(l.acceptTasks, sealtasks.TTCommit2)
	//}

	diskUsed, apDiskUsed := l.localDiskUsed()
	//log.Debugf(" worker %s DiskUsed: %d (hold space %d * 32GB )", hostname, diskUsed, diskUsed)
	//var diskUsed = uint64(0)
	//var apDiskUsed = uint64(0)
	//if !l.isMiner {
	//	diskUsed, apDiskUsed = l.localDiskUsed()
	//}
	gpus, err := ffi.GetGPUDevices()
	if err != nil {
		log.Errorf("getting gpu devices failed: %+v", err)
	}

	h, err := sysinfo.Host()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting host info: %w", err)
	}

	mem, err := h.Memory()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting memory info: %w", err)
	}

	memSwap := mem.VirtualTotal
	if l.noSwap {
		memSwap = 0
	}

	return storiface.WorkerInfo{
		Hostname: hostname,
		Resources: storiface.WorkerResources{
			MemPhysical: mem.Total,
			MemSwap:     memSwap,
			MemReserved: mem.VirtualUsed + mem.Total - mem.Available, // TODO: sub this process
			CPUs:        uint64(runtime.NumCPU()),
			GPUs:        gpus,

			IsMiner:            l.isMiner,
			AddPieceMax:        addPieceMax,
			PreCommit1Max:      preCommit1Max,
			DiskHoldMax:        diskHoldMax,
			APDiskHoldMax:      apDiskHoldMax,
			PreCommit2Max:      preCommit2Max,
			Commit2Max:         commit2Max,
			AllowMyScheduler:   l.allowMyScheduler,
			ForceP1FromLocalAP: forceP1FromLocalAP,
			ForceP2FromLocalP1: forceP2FromLocalP1,
			ForceC2FromLocalP2: forceC2FromLocalP2,
			IsPlanOffline:      isPlanOffline,
			AllowP2C2Parallel:  allowP2C2Parallel,
			AutoPledgeDiff:     autoPledgeDiff,
			DiskUsed:           diskUsed,
			APDiskUsed:         apDiskUsed,
			IgnoreOutOfSpace:	ignoreOutOfSpace,
		},
	}, nil
}

func (l *LocalWorker) Session(ctx context.Context) (uuid.UUID, error) {
	if atomic.LoadInt64(&l.testDisable) == 1 {
		return uuid.UUID{}, xerrors.Errorf("disabled")
	}

	select {
	case <-l.closing:
		return ClosedWorkerID, nil
	default:
		return l.session, nil
	}
}

func (l *LocalWorker) Close() error {
	close(l.closing)
	return nil
}

// WaitQuiet blocks as long as there are tasks running
func (l *LocalWorker) WaitQuiet() {
	l.running.Wait()
}

type wctx struct {
	vals    context.Context
	closing chan struct{}
}

func (w *wctx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (w *wctx) Done() <-chan struct{} {
	return w.closing
}

func (w *wctx) Err() error {
	select {
	case <-w.closing:
		return context.Canceled
	default:
		return nil
	}
}

func (w *wctx) Value(key interface{}) interface{} {
	return w.vals.Value(key)
}

var _ context.Context = &wctx{}

var _ Worker = &LocalWorker{}
