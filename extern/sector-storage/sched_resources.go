package sectorstorage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

func (a *activeResources) withResourcesMeNoLocker(index stores.SectorIndex, allowMyScheduler bool, sector abi.SectorID,taskType sealtasks.TaskType,worker *workerHandle,
	id WorkerID, wr storiface.WorkerResources, r Resources, cb func() error) error {
	a.add(allowMyScheduler, taskType, wr, r)

	err := cb()

	a.free(allowMyScheduler, taskType, wr, r)
	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

func (a *activeResources) withResourcesMe(index stores.SectorIndex, allowMyScheduler bool, sector abi.SectorID,taskType sealtasks.TaskType,worker *workerHandle,
	id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	//info, errInfo := worker.workerRpc.Info(context.TODO())
	//if errInfo != nil {
	//	log.Debugf("withResourcesMe worker.workerRpc.Info get err")
	//}

	for !a.canHandleRequestMine(index, sector, taskType, worker, id, "withResources") {
		log.Debugf("withResourcesMe didn't find any good withResources on worker %s@%s for sector s-t0%d-%d taskType %s",hex.EncodeToString(id[:]), worker.info.Hostname, sector.Miner, sector.Number, taskType)
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	a.add(allowMyScheduler, taskType, wr, r)

	err := cb()

	a.free(allowMyScheduler, taskType, wr, r)
	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

func (a *activeResources) withResources(allowMyScheduler bool, taskType sealtasks.TaskType,
id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	for !a.canHandleRequest(r, id, "withResources", wr) {
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	a.add(allowMyScheduler, taskType, wr, r)

	err := cb()

	a.free(allowMyScheduler, taskType, wr, r)
	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

func (a *activeResources) add(allowMyScheduler bool, taskType sealtasks.TaskType, wr storiface.WorkerResources, r Resources) {
	if allowMyScheduler {
		if taskType == sealtasks.TTFinalize {

		} else if taskType == sealtasks.TTCommit2 {
			a.commit2Used += 1
		} else if taskType == sealtasks.TTCommit1 {

		} else if taskType == sealtasks.TTPreCommit2 {
			a.preCommit2Used += 1
		} else if taskType == sealtasks.TTPreCommit1 {
			a.preCommit1Used += 1
		} else if taskType == sealtasks.TTAddPiece {
			a.apUsed += 1
		}
	}
	if r.CanGPU {
		a.gpuUsed = true
	}
	a.cpuUse += r.Threads(wr.CPUs)
	a.memUsedMin += r.MinMemory
	a.memUsedMax += r.MaxMemory
}

func (a *activeResources) free(allowMyScheduler bool, taskType sealtasks.TaskType, wr storiface.WorkerResources, r Resources) {
	if allowMyScheduler {
		if taskType == sealtasks.TTFinalize {

		} else if taskType == sealtasks.TTCommit2 {
			a.commit2Used -= 1
		} else if taskType == sealtasks.TTCommit1 {

		} else if taskType == sealtasks.TTPreCommit2 {
			a.preCommit2Used -= 1
		} else if taskType == sealtasks.TTPreCommit1 {
			a.preCommit1Used -= 1
		} else if taskType == sealtasks.TTAddPiece {
			a.apUsed -= 1
		}
	}

	if r.CanGPU {
		a.gpuUsed = false
	}
	a.cpuUse -= r.Threads(wr.CPUs)
	a.memUsedMin -= r.MinMemory
	a.memUsedMax -= r.MaxMemory
}

func (a *activeResources) canHandleRequest(needRes Resources, wid WorkerID, caller string, res storiface.WorkerResources) bool {

	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + a.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	if minNeedMem > res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough physical memory - need: %dM, have %dM", wid, caller, minNeedMem/mib, res.MemPhysical/mib)
		return false
	}

	maxNeedMem := res.MemReserved + a.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory

	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough virtual memory - need: %dM, have %dM", wid, caller, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib)
		return false
	}

	if a.cpuUse+needRes.Threads(res.CPUs) > res.CPUs {
		log.Debugf("sched: not scheduling on worker %s for %s; not enough threads, need %d, %d in use, target %d", wid, caller, needRes.Threads(res.CPUs), a.cpuUse, res.CPUs)
		return false
	}

	if len(res.GPUs) > 0 && needRes.CanGPU {
		if a.gpuUsed {
			log.Debugf("sched: not scheduling on worker %s for %s; GPU in use", wid, caller)
			return false
		}
	}

	return true
}

func (a *activeResources) canHandleRequestMine(index stores.SectorIndex, sector abi.SectorID,taskType sealtasks.TaskType,worker *workerHandle, wid2 WorkerID, caller string) bool {
	//func (a *activeResources) canHandleRequestMine(index stores.SectorIndex, sector abi.SectorID,taskType sealtasks.TaskType,worker *workerHandle, wid2 WorkerID, caller string, info storiface.WorkerInfo) bool {
	wid := hex.EncodeToString(wid2[:]) //hex.EncodeToString(wid2[5:])
	if worker == nil || worker.workerRpc == nil {
		log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s for %s from %s; Maybe it's offline.", sector.Miner, sector.Number, wid, taskType, caller)
		return false
	}
	info, err := worker.workerRpc.Info(context.TODO())
	if err != nil {
		return false
	}

	apMax := info.Resources.AddPieceMax
	p1Max := info.Resources.PreCommit1Max                   // res.PreCommit1Max
	p2Max := info.Resources.PreCommit2Max                   // res.PreCommit2Max
	c2Max := info.Resources.Commit2Max                      // res.Commit2Max
	diskHoldMax := info.Resources.DiskHoldMax               // res.DiskHoldMax
	apDiskHoldMax := info.Resources.APDiskHoldMax           // res.APDiskHoldMax
	forceP1FromLocalAP := info.Resources.ForceP1FromLocalAP // res.ForceP1FromLocalAP
	forceP2FromLocalP1 := info.Resources.ForceP2FromLocalP1 // res.ForceP2FromLocalP1
	forceC2FromLocalP2 := info.Resources.ForceC2FromLocalP2 // res.ForceC2FromLocalP2
	isPlanOffline := info.Resources.IsPlanOffline
	diskUsed := info.Resources.DiskUsed
	apDiskUsed := info.Resources.APDiskUsed
	allowP2C2Parallel := info.Resources.AllowP2C2Parallel
	//ignoreOutOfSpace := info.Resources.IgnoreOutOfSpace
	isDiskConstraints := false
	//autoPledgeDiff := info.Resources.AutoPledgeDiff

	//if p1Max == 0 {
	//	p1Max = 7
	//}
	//if p2Max == 0 {
	//	p2Max = 1
	//}
	//if c2Max == 0 {
	//	c2Max = 1
	//}
	//if apMax == 0 {
	//	apMax = 5
	//}
	if apMax == 0 && p1Max == 0 && p2Max ==0 && c2Max ==0 && diskHoldMax ==0 && apDiskHoldMax ==0 && 
		isPlanOffline == false	&& forceP1FromLocalAP == false && forceP2FromLocalP1 == false && 
		forceC2FromLocalP2 == false && allowP2C2Parallel == false {
		minerPath, ok := os.LookupEnv("LOTUS_MINER_PATH")
		if ok {
			mb, errIgnore := ioutil.ReadFile(filepath.Join(minerPath, "testOpenSource.json"))
			if errIgnore == nil {
				var meta stores.TestSchedulerMeta
				if errIgnore := json.Unmarshal(mb, &meta); errIgnore == nil {
					apMax = meta.AddPieceMax
					p1Max = meta.PreCommit1Max
					p2Max = meta.PreCommit2Max
					c2Max = meta.Commit2Max
					forceP1FromLocalAP = meta.ForceP1FromLocalAP
					forceP2FromLocalP1 = meta.ForceP2FromLocalP1
					forceC2FromLocalP2 = meta.ForceC2FromLocalP2
					allowP2C2Parallel = meta.AllowP2C2Parallel
				}
			}
		}
	}

	if diskHoldMax > 0 && diskUsed > diskHoldMax {
		log.Debugf("worker %s@%s not enough DiskHold quota, %d in use, DiskHoldMax is %d,it is forced into disk constraints mode", wid, info.Hostname, diskUsed, diskHoldMax)
		isDiskConstraints = true //isPlanOffline = true
		//return false
	}

	//if ignoreOutOfSpace {
	//	log.Debugf("worker %s@%s not enough DiskHold quota, %d in use, DiskHoldMax is %d,ignoreOutOfSpace is true,it is forced into disk constraints mode", wid, info.Hostname, diskUsed, diskHoldMax)
	//	isDiskConstraints = true //isPlanOffline = true
	//	//return false
	//}

	if taskType == sealtasks.TTPreCommit1 {
		if isPlanOffline {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It plans offline maintenance", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
			return false
		}
		if isDiskConstraints {
			var p1Local = a.localCacheExist(index, sector, worker, storiface.FTCache)
			if !p1Local {
				log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It is forced into disk constraints mode", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
				return false
			}
		}
		if diskHoldMax > 0 && diskUsed+14 > diskHoldMax {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; not enough DiskHold quota, %d in use, need 14 more, DiskHoldMax is %d", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller, diskUsed, diskHoldMax)
			return false
		}

		if a.preCommit1Used >= p1Max {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; not enough PreCommit1 quota, 1 need, %d in use, PreCommit1Max is %d", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller, a.preCommit1Used, p1Max)
			return false
		}

		if (forceP1FromLocalAP == true) {
			var apLocal = a.localCacheExist(index, sector, worker, storiface.FTUnsealed)
			if apLocal == false {
				log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It only receives AP from the local.", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
				return false
			}
		}

	} else if taskType == sealtasks.TTPreCommit2 {
		if isPlanOffline {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It plans offline maintenance", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
			return false
		}
		if isDiskConstraints {
			var p1Local = a.localCacheExist(index, sector, worker, storiface.FTCache)
			if p1Local == false {
				log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It is forced into disk constraints mode", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
				return false
			}
		}
		if a.preCommit2Used >= p2Max {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; not enough PreCommit2 quota, 1 need, %d in use, PreCommit2Max is %d", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller, a.preCommit2Used, p2Max)
			return false
		}
		if (forceP2FromLocalP1 == true) {
			var p1Local = a.localCacheExist(index, sector, worker, storiface.FTCache)
			if p1Local == false {
				log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It only receives P1 from the local.", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
				return false
			}
		}
		if allowP2C2Parallel == false && a.commit2Used >= 1 {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It already has a C2 running and does not receive P2 task", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
			return false
		}
	} else if taskType == sealtasks.TTCommit1 {
		//if isPlanOffline {
		//	log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It plans offline maintenance", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
		//	return false
		//}
		//if (forceC2FromLocalP2 == true) {
		//	var apLocal = a.localCacheExist(index, sector, worker, storiface.FTCache)
		//	if apLocal == false {
		//		log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It only receives P2 from the local.", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
		//		return false
		//	}
		//}
	} else if taskType == sealtasks.TTCommit2 {
		if isPlanOffline {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It plans offline maintenance", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
			return false
		}
		//if isDiskConstraints {
		//	var p1Local = a.localCacheExist(index, sector, worker, storiface.FTCache)
		//	if !p1Local {
		//		log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s; It is forced into disk constraints mode", sector.Miner, sector.Number, wid, info.Hostname, taskType)
		//		return false
		//	}
		//}
		if a.commit2Used >= c2Max {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; not enough Commit2 quota, 1 need, %d in use, Commit2Max is %d", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller, a.commit2Used, c2Max)
			return false
		}
		if (forceC2FromLocalP2 == true) {
			var apLocal = a.localCacheExist(index, sector, worker, storiface.FTCache)
			if apLocal == false {
				log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It only receives P2 from the local.", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
				return false
			}
		}
		if allowP2C2Parallel == false && a.preCommit2Used >= 1 {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It already has a P2 running and does not receive C2 task", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
			return false
		}
	} else if taskType == sealtasks.TTAddPiece {
		if isPlanOffline {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It plans offline maintenance", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
			return false
		}
		if isDiskConstraints {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; It is forced into disk constraints mode", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller)
			return false
		}
		if a.apUsed >= apMax {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; not enough AddPiece quota, 1 need, %d in use, AddPieceMax is %d", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller, a.apUsed, apMax)
			return false
		}
		if diskHoldMax > 0 && diskUsed+1 > diskHoldMax {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; not enough DiskHold quota, %d in use, need 1 more, DiskHoldMax is %d", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller, diskUsed, diskHoldMax)
			return false
		}
		if apDiskHoldMax > 0 && apDiskUsed+1 > apDiskHoldMax {
			log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s from %s; not enough APDiskHold quota, %d in use, need 1 more, APDiskHoldMax is %d", sector.Miner, sector.Number, wid, info.Hostname, taskType, caller, apDiskUsed, apDiskHoldMax)
			return false
		}
		//if a.preCommit2Used >= 7023 {
		//	log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s; It already has a P2 running and does not receive AddPiece task", sector.Miner, sector.Number, wid, info.Hostname, taskType)
		//	return false
		//}
		//if a.commit2Used >= 1023 {
		//	log.Debugf("sector s-t0%d-%d sched: not scheduling on worker %s@%s for %s; It already has a C2 running and does not receive AddPiece task", sector.Miner, sector.Number, wid, info.Hostname, taskType)
		//	return false
		//}
	}
	return true
}

func (a *activeResources) localCacheCount(index stores.SectorIndex,sector abi.SectorID,worker *workerHandle) uint64 {
	var cacheCount = uint64(0)
	var out []stores.StoragePath
	out, err := worker.workerRpc.Paths(context.TODO())
	if err != nil {
		log.Error(err)
		return cacheCount
	}
	for _, path := range out {
		unsealed, err := index.StorageFindSector(context.TODO(), sector, storiface.FTUnsealed, 0, false)
		if err != nil {
			log.Debugf("continue,canHandleRequestMine finding unsealed errored: %w", err)
			continue
		}
		for _, info := range unsealed {
			if path.ID == info.ID{
				cacheCount++
			}
		}

		sealed, err := index.StorageFindSector(context.TODO(), sector, storiface.FTSealed, 0, false)
		if err != nil {
			log.Debugf("continue,canHandleRequestMine finding sealed errored: %w", err)
			continue
		}
		for _, info := range sealed {
			if path.ID == info.ID{
				cacheCount++
			}
		}

		cached, err := index.StorageFindSector(context.TODO(), sector, storiface.FTCache, 0, false)
		if err != nil {
			log.Debugf("continue,canHandleRequestMine finding cache errored: %w", err)
			continue
		}
		for _, info := range cached {
			if path.ID == info.ID{
				cacheCount = cacheCount + 14
			}
		}
	}
	return cacheCount
}
func (a *activeResources) localCacheExist(index stores.SectorIndex,sector abi.SectorID,worker *workerHandle, fileType storiface.SectorFileType) bool {
	var localExist = false
	var out []stores.StoragePath
	out, err := worker.workerRpc.Paths(context.TODO())
	if err != nil {
		log.Error(err)
		return false
	}
OuterLoop:
	for _, path := range out {
		//log.Debugf("path.LocalPath: %s on worker %s for s-t0%d-%d", path.LocalPath, wid, sector.Miner, sector.Number)
		cached, err := index.StorageFindSector(context.TODO(), sector, fileType, 0, false)
		if err != nil {
			log.Debugf("continue,canHandleRequestMine finding cache errored: %w", err)
			continue
		}
		for _, info := range cached {
			if path.ID == info.ID {
				localExist = true
				break OuterLoop
			}
		}
	}
	return localExist
}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	if memMin > max {
		max = memMin
	}

	memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	if memMax > max {
		max = memMax
	}

	return max
}

func (wh *workerHandle) utilization() float64 {
	wh.lk.Lock()
	u := wh.active.utilization(wh.info.Resources)
	u += wh.preparing.utilization(wh.info.Resources)
	wh.lk.Unlock()
	wh.wndLk.Lock()
	for _, window := range wh.activeWindows {
		u += window.allocated.utilization(wh.info.Resources)
	}
	wh.wndLk.Unlock()

	return u
}

func (wh *workerHandle) utilizationMine(taskType sealtasks.TaskType, a, b *workerHandle) bool {
	//better := a.utilization() < b.utilization()
	var better = false
	if taskType == sealtasks.TTFinalize {

	} else if taskType == sealtasks.TTCommit2 {
		better = (a.preparing.commit2Used + a.active.commit2Used) < ( b.preparing.commit2Used + b.active.commit2Used )
	} else if taskType == sealtasks.TTCommit1 {
		better = (a.preparing.commit2Used + a.active.commit2Used) < ( b.preparing.commit2Used + b.active.commit2Used )
		//if (a.preparing.commit2Used + a.active.commit2Used) > 0{
		//	better = false
		//}
	} else if taskType == sealtasks.TTPreCommit2 {
		better = (a.preparing.preCommit2Used + a.active.preCommit2Used) < ( b.preparing.preCommit2Used + b.active.preCommit2Used)
		//if (a.preparing.commit2Used + a.active.commit2Used) > 0{
		//	better = false
		//}
	} else if taskType == sealtasks.TTPreCommit1 {
		better = (a.preparing.preCommit1Used + a.active.preCommit1Used) < ( b.preparing.preCommit1Used + b.active.preCommit1Used)
	} else if taskType == sealtasks.TTAddPiece {
		better = (a.preparing.apUsed + a.active.apUsed) < ( b.preparing.apUsed + b.active.apUsed)
		//better = (a.preparing.preCommit1Used + a.active.preCommit1Used + a.preparing.apUsed + a.active.apUsed) < ( b.preparing.preCommit1Used + b.active.preCommit1Used + b.preparing.apUsed + b.active.apUsed)
		//if (a.info.Resources.IsMiner) {
		//	better = false
		//}
		//if (b.info.Resources.IsMiner) {
		//	better = true
		//}
		//if (a.preparing.preCommit2Used + a.active.preCommit2Used) > 0{
		//	better = false
		//}
		//if (a.preparing.commit2Used + a.active.commit2Used) > 0{
		//	better = false
		//}
	}

	return better
}