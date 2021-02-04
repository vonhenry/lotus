package sectorstorage

import (
	"context"
	"encoding/hex"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 0
)

func getPriorityByTask(taskType sealtasks.TaskType) int {
	var priority = 0
	if taskType == sealtasks.TTFinalize{
		priority = 900
	}else if taskType == sealtasks.TTCommit2{
		priority = 800
	}else if taskType == sealtasks.TTCommit1{
		priority = 700
	}else if taskType == sealtasks.TTPreCommit2{
		priority = 600
	}else if taskType == sealtasks.TTPreCommit1{
		priority = 500
	}else if taskType == sealtasks.TTAddPiece{
		priority = 400
	}

	return priority
}

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle, allowMyScheduler bool) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing

	allowMyScheduler  bool
	preCommit1Max     uint64
	preCommit1HoldMax uint64
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	preCommit1Used uint64
	preCommit2Used uint64
	commit2Used    uint64
	apUsed         uint64

	cond *sync.Cond
}

type workerRequest struct {
	ix stores.SectorIndex
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler(allowMyScheduler bool) *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),

		allowMyScheduler: allowMyScheduler,
		//preCommit1Max:    preCommit1Max,
		//diskHoldMax:      diskHoldMax,
	}
}

func (sh *scheduler) Schedule(ctx context.Context, index stores.SectorIndex ,sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		ix:       index,
		sector:   sector,
		taskType: taskType,
		//priority: getPriority(ctx),
		priority: getPriorityByTask(taskType),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *scheduler) runAutoPledge() {
	for {
		select {
		case <-time.After(30 * time.Second):
			//sh.workersLk.Lock()
			//defer sh.workersLk.Unlock()
			sh.workersLk.RLock()
			defer sh.workersLk.RUnlock()
			var apUsed = uint64(0)
			var apMax = uint64(0)
			var p1Used = uint64(0)
			var p1Max = uint64(0)
			for _, worker := range sh.workers {
				if worker == nil || !worker.enabled || worker.workerRpc == nil {
					continue
				}
				info, err := worker.workerRpc.Info(context.TODO())
				if err != nil {
					continue
				}

				tasks, err := worker.workerRpc.TaskTypes(context.TODO())
				if err != nil {
					continue //return false, xerrors.Errorf("getting supported worker task types: %w", err)
				}
				_, supported := tasks[sealtasks.TTAddPiece]
				if supported {
					apUsed = apUsed + worker.preparing.apUsed + worker.active.apUsed
					apMax = apMax + info.Resources.AddPieceMax
				}

				_, supported = tasks[sealtasks.TTPreCommit1]
				if supported {
					p1Used = p1Used + worker.preparing.preCommit1Used + worker.active.preCommit1Used
					p1Max = p1Max + info.Resources.PreCommit1Max
				}
			}
			if p1Used >= p1Max {
				return
			}
			if apUsed >= apMax {
				return
			}
			if p1Max - p1Used > apMax - apUsed {
				log.Infof(" lotus-miner sectors pledge automatic delivery")
				//exe, err := os.Executable()
				//if err != nil {
				//	log.Errorf("getting executable for auto-restart: %+v", err)
				//}
				//_ = log.Sync()
				//if err := syscall.Exec(exe, []string{exe,
				//	" sectors ",
				//	" pledge ",
				//}, os.Environ()); err != nil {
				//	log.Errorf("auto sectors pledge error: %+v", err)	//fmt.Println(err)
				//}

			}
		}
	}
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						sh.schedQueue.Push(request)
					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			if sh.allowMyScheduler {
				sh.trySchedMine()
			} else {
				sh.trySched()
			}
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySchedMine() {
	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()
	log.Debugf("trySchedMine SCHED %d queued; %d workers", sh.schedQueue.Len(), len(sh.workers))
	//if(sh.schedQueue.Len() > 1) {
	//	sort.SliceStable(sh.schedQueue, func(i, j int) bool {
	//		return (*sh.schedQueue)[i].priority < (*sh.schedQueue)[j].priority
	//	})
	//}

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]
		tried := 0
		var acceptable []WorkerID
		//needRes := ResourceTable[task.taskType][sh.spt]
		for wid, worker := range sh.workers {
			if !worker.enabled {
				log.Debugf("trySchedMine skipping disabled worker %s", worker.info.Hostname)
				continue
			}
			//info, err := worker.workerRpc.Info(context.TODO())
			//if err != nil {
			//	log.Debugf("trySchedMine worker.workerRpc.Info get err for %s", worker.info.Hostname)
			//	continue
			//}
			rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
			ok, err := task.sel.Ok(rpcCtx, task.taskType, task.sector.ProofType, worker)
			cancel()
			if err != nil {
				log.Errorf("trySchedMine task.sel.Ok(worker: %s) error: %+v", worker.info.Hostname, err)
				continue
			}
			if !ok {
				continue
			}
			if !worker.preparing.canHandleRequestMine(task.ix, task.sector.ID, task.taskType, worker, wid,"schedAcceptable") {
				continue
			}
			tried++
			acceptable = append(acceptable, wid)
		}

		if len(acceptable) > 0 {
			sort.SliceStable(acceptable, func(i, j int) bool {
				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()
				r, err := task.sel.Cmp(rpcCtx, task.taskType, sh.workers[acceptable[i]], sh.workers[acceptable[j]], sh.allowMyScheduler)

				if err != nil {
					log.Error("trySchedMine task.sel.Cmp selecting best worker: %s", err)
				}
				return r
			})

			sh.schedQueue.Remove(sqi)
			sqi--
			taskDone := make(chan struct{}, 1)
			sh.assignWorker(taskDone, acceptable[0], sh.workers[acceptable[0]], task)
			//sh.assignWorkerNoLocker(taskDone, acceptable[0], sh.workers[acceptable[0]], task)
			widstring := hex.EncodeToString(acceptable[0][:]) //hex.EncodeToString(acceptable[0][5:])
			//log.Infof("trySchedMine has successfully scheduled for sector s-t0%d-%d taskType %s to worker %d",task.sector.Miner,task.sector.Number,task.taskType,acceptable[0])
			log.Infof("trySchedMine has successfully scheduled for sector s-t0%d-%d taskType %s to worker %s", task.sector.ID.Miner, task.sector.ID.Number, task.taskType, widstring)
		}
		if tried == 0 {
			log.Debugf("trySchedMine didn't find any good workers for sector s-t0%d-%d taskType %s",task.sector.ID.Miner,task.sector.ID.Number,task.taskType)
		}

	}
}

func (sh *scheduler) assignWorkerNoLocker(taskDone chan struct{}, wid WorkerID, w *workerHandle, req *workerRequest) error {
	//w, sh := sw.worker, sw.sched

	needRes := ResourceTable[req.taskType][req.sector.ProofType]

	w.lk.Lock()
	w.preparing.add(sh.allowMyScheduler,req.taskType, w.info.Resources, needRes)
	w.lk.Unlock()

	go func() {
		// first run the prepare step (e.g. fetching sector data from other worker)
		err := req.prepare(req.ctx, sh.workTracker.worker(wid, w.workerRpc))
		//sh.workersLk.Lock()

		if err != nil {
			w.lk.Lock()
			w.preparing.free(sh.allowMyScheduler, req.taskType, w.info.Resources, needRes)
			w.lk.Unlock()
			//sh.workersLk.Unlock()

			select {
			case taskDone <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		// wait (if needed) for resources in the 'active' window
		//err = w.active.withResources(sh.allowMyScheduler, req.taskType,
		//	wid, w.info.Resources, needRes, &sh.workersLk, func() error {
		err = w.active.withResourcesMeNoLocker(req.ix, sh.allowMyScheduler, req.sector.ID, req.taskType, w,
					wid, w.info.Resources, needRes, func() error {
				w.lk.Lock()
				w.preparing.free(sh.allowMyScheduler, req.taskType, w.info.Resources, needRes)
				w.lk.Unlock()
				//sh.workersLk.Unlock()
				//defer sh.workersLk.Lock() // we MUST return locked from this function

				select {
				case taskDone <- struct{}{}:
				case <-sh.closing:
				}

				// Do the work!
				err = req.work(req.ctx, sh.workTracker.worker(wid, w.workerRpc))

				select {
				case req.ret <- workerResponse{err: err}:
				case <-req.ctx.Done():
					log.Warnf("request got cancelled before we could respond")
				case <-sh.closing:
					log.Warnf("scheduler closed while sending response")
				}

				return nil
			})

		//sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (sh *scheduler) assignWorker(taskDone chan struct{}, wid WorkerID, w *workerHandle, req *workerRequest) error {
	//w, sh := sw.worker, sw.sched

	needRes := ResourceTable[req.taskType][req.sector.ProofType]

	w.lk.Lock()
	w.preparing.add(sh.allowMyScheduler,req.taskType, w.info.Resources, needRes)
	w.lk.Unlock()

	go func() {
		// first run the prepare step (e.g. fetching sector data from other worker)
		err := req.prepare(req.ctx, sh.workTracker.worker(wid, w.workerRpc))
		sh.workersLk.Lock()

		if err != nil {
			w.lk.Lock()
			w.preparing.free(sh.allowMyScheduler, req.taskType, w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()

			select {
			case taskDone <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		// wait (if needed) for resources in the 'active' window
		//err = w.active.withResources(sh.allowMyScheduler, req.taskType,
		//	wid, w.info.Resources, needRes, &sh.workersLk, func() error {
		err = w.active.withResourcesMe(req.ix, sh.allowMyScheduler, req.sector.ID, req.taskType, w,
			wid, w.info.Resources, needRes, &sh.workersLk, func() error {
				w.lk.Lock()
				w.preparing.free(sh.allowMyScheduler, req.taskType, w.info.Resources, needRes)
				w.lk.Unlock()
				sh.workersLk.Unlock()
				defer sh.workersLk.Lock() // we MUST return locked from this function

				select {
				case taskDone <- struct{}{}:
				case <-sh.closing:
				}

				// Do the work!
				err = req.work(req.ctx, sh.workTracker.worker(wid, w.workerRpc))

				select {
				case req.ret <- workerResponse{err: err}:
				case <-req.ctx.Done():
					log.Warnf("request got cancelled before we could respond")
				case <-sh.closing:
					log.Warnf("scheduler closed while sending response")
				}

				return nil
			})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	windowsLen := len(sh.openWindows)
	queuneLen := sh.schedQueue.Len()

	log.Debugf("SCHED %d queued; %d open windows", queuneLen, windowsLen)

	if windowsLen == 0 || queuneLen == 0 {
		// nothing to schedule on
		return
	}

	windows := make([]schedWindow, windowsLen)
	acceptableWindows := make([][]int, queuneLen)

	// Step 1
	throttle := make(chan struct{}, windowsLen)

	var wg sync.WaitGroup
	wg.Add(queuneLen)
	for i := 0; i < queuneLen; i++ {
		throttle <- struct{}{}

		go func(sqi int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]
			needRes := ResourceTable[task.taskType][task.sector.ProofType]

			task.indexHeap = sqi
			for wnd, windowRequest := range sh.openWindows {
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				if !worker.enabled {
					log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
					continue
				}

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, task.sector.ProofType, worker)
				cancel()
				if err != nil {
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
			}

			if len(acceptableWindows[sqi]) == 0 {
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj, sh.allowMyScheduler)
				if err != nil {
					log.Errorf("selecting best worker: %s", err)
				}
				return r
			})
		}(i)
	}

	wg.Wait()

	log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0
	rmQueue := make([]int, 0, queuneLen)

	for sqi := 0; sqi < queuneLen; sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][task.sector.ProofType]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			wr := sh.workers[wid].info.Resources

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.ID.Number, wnd)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
				continue
			}

			log.Debugf("SCHED ASSIGNED sqi:%d sector %d task %s to window %d", sqi, task.sector.ID.Number, task.taskType, wnd)

			windows[wnd].allocated.add(sh.allowMyScheduler, task.taskType, wr, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		rmQueue = append(rmQueue, sqi)
		scheduled++
	}

	if len(rmQueue) > 0 {
		for i := len(rmQueue) - 1; i >= 0; i-- {
			sh.schedQueue.Remove(rmQueue[i])
		}
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, windowsLen-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
