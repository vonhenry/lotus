package sectorstorage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (m *Manager) WorkerStats() map[uuid.UUID]storiface.WorkerStats {
	m.sched.workersLk.RLock()
	defer m.sched.workersLk.RUnlock()

	out := map[uuid.UUID]storiface.WorkerStats{}

	var apMaxDefault = uint64(1)
	var p1MaxDefault = uint64(1)
	var p2MaxDefault = uint64(1)
	var c2MaxDefault = uint64(1)
	//var isExternal = false
	//var forceP1FromLocalAPDefault = true
	//var forceP2FromLocalP1Default = true
	//var forceC2FromLocalP2Default = false
	//var allowP2C2ParallelDefault = true
	minerPath, ok := os.LookupEnv("LOTUS_MINER_PATH")
	if ok {
		mb, errIgnore := ioutil.ReadFile(filepath.Join(minerPath, "testOpenSource.json"))
		if errIgnore == nil {
			var meta stores.TestSchedulerMeta
			if errIgnore := json.Unmarshal(mb, &meta); errIgnore == nil {
				apMaxDefault = meta.AddPieceMax
				p1MaxDefault = meta.PreCommit1Max
				p2MaxDefault = meta.PreCommit2Max
				c2MaxDefault = meta.Commit2Max
				//forceP1FromLocalAPDefault = meta.ForceP1FromLocalAP
				//forceP2FromLocalP1Default = meta.ForceP2FromLocalP1
				//forceC2FromLocalP2Default = meta.ForceC2FromLocalP2
				//allowP2C2ParallelDefault = meta.AllowP2C2Parallel
			}
		}
	}

	for id, handle := range m.sched.workers {
		var types string

		var tasks string
		var apTasks string
		var p1Tasks string
		var p2Tasks string
		var c2Tasks string
		var apRunning string
		var p1Running string
		var p2Running string
		var c2Running string
		var apPreparing string
		var p1Preparing string
		var p2Preparing string
		var c2Preparing string
		allMax := uint64(0)
		allUsed := uint64(0)

		apMax := handle.info.Resources.AddPieceMax
		p1Max := handle.info.Resources.PreCommit1Max
		p2Max := handle.info.Resources.PreCommit2Max
		c2Max := handle.info.Resources.Commit2Max
		isPlanOffline := false
		info, err := handle.workerRpc.Info(context.TODO())
		if err == nil {
			apMax = info.Resources.AddPieceMax
			p1Max = info.Resources.PreCommit1Max
			p2Max = info.Resources.PreCommit2Max
			c2Max = info.Resources.Commit2Max
			isPlanOffline = info.Resources.IsPlanOffline
		}
		taskTypes, err := handle.workerRpc.TaskTypes(context.TODO())
		//if p1Max == 0 {
		//	p1Max = 7
		//}
		//if p2Max == 0 {
		//	p2Max = 1
		//}
		//if c2Max == 0 {
		//	c2Max = 1
		//}
		//isExternal = false
		if apMax == 0 && p1Max == 0 && p2Max ==0 && c2Max ==0 && info.Resources.DiskHoldMax ==0 && info.Resources.APDiskHoldMax ==0 && 
			isPlanOffline == false && info.Resources.ForceP1FromLocalAP == false && info.Resources.ForceP2FromLocalP1 == false && 
			info.Resources.ForceC2FromLocalP2 == false && info.Resources.AllowP2C2Parallel == false {
			apMax = apMaxDefault
			p1Max = p1MaxDefault
			p2Max = p2MaxDefault
			c2Max = c2MaxDefault
			//isExternal = true
		}
		if err == nil {
			for k, _ := range taskTypes {
				types = types + strings.Replace(string(k), "seal/v0/", "", -1) + ","

				if k == sealtasks.TTCommit2 {
					if c2Max > (handle.active.commit2Used + handle.preparing.commit2Used) {
						c2Tasks = fmt.Sprintf(" %d C2 ", c2Max-(handle.active.commit2Used+handle.preparing.commit2Used))
					}
					if  (handle.active.commit2Used) > 0{
						c2Running = fmt.Sprintf(" %d C2 ", (handle.active.commit2Used))
					}
					if  (handle.preparing.commit2Used) > 0{
						c2Preparing = fmt.Sprintf(" %d C2 ", (handle.preparing.commit2Used))
					}
					allMax = allMax + c2Max
					allUsed = allUsed + handle.active.commit2Used + handle.preparing.commit2Used
				} else if k == sealtasks.TTPreCommit2 {
					if p2Max > (handle.active.preCommit2Used + handle.preparing.preCommit2Used) {
						p2Tasks = fmt.Sprintf(" %d P2 ", p2Max-(handle.active.preCommit2Used+handle.preparing.preCommit2Used))
					}
					if (handle.active.preCommit2Used) > 0 {
						p2Running = fmt.Sprintf(" %d P2 ", (handle.active.preCommit2Used))
					}
					if (handle.preparing.preCommit2Used) > 0 {
						p2Preparing = fmt.Sprintf(" %d P2 ", (handle.preparing.preCommit2Used))
					}
					allMax = allMax + p2Max
					allUsed = allUsed + handle.active.preCommit2Used + handle.preparing.preCommit2Used
				} else if k == sealtasks.TTPreCommit1 {
					if p1Max > (handle.active.preCommit1Used + handle.preparing.preCommit1Used) {
						p1Tasks = fmt.Sprintf(" %d P1 ", p1Max-(handle.active.preCommit1Used+handle.preparing.preCommit1Used))
					}
					if (handle.active.preCommit1Used) > 0{
						p1Running = fmt.Sprintf(" %d P1 ", (handle.active.preCommit1Used))
					}
					if (handle.preparing.preCommit1Used) > 0{
						p1Preparing = fmt.Sprintf(" %d P1 ", (handle.preparing.preCommit1Used))
					}
					allMax = allMax + p1Max
					allUsed = allUsed + handle.active.preCommit1Used + handle.preparing.preCommit1Used
				} else if k == sealtasks.TTAddPiece {
					if apMax > (handle.active.apUsed + handle.preparing.apUsed) {
						apTasks = fmt.Sprintf(" %d AP ", apMax-(handle.active.apUsed+handle.preparing.apUsed))
					}
					if (handle.active.apUsed) > 0 {
						apRunning = fmt.Sprintf(" %d AP ", handle.active.apUsed)
					}
					if (handle.preparing.apUsed) > 0 {
						apPreparing = fmt.Sprintf(" %d AP ", handle.preparing.apUsed)
					}
					allMax = allMax + apMax
					allUsed = allUsed + handle.active.apUsed + handle.preparing.apUsed
				}
			}
			//if isExternal {
			//	types = types + " <External Worker> "
			//}
			if isPlanOffline {
				tasks = fmt.Sprintf("Running: %s %s %s %s | Preparing: %s %s %s %s | DiskUsed: %d APDiskUsed: %d, I plan to enter maintenance mode and no longer receive new tasks. ", apRunning, p1Running, p2Running, c2Running, apPreparing, p1Preparing, p2Preparing, c2Preparing, info.Resources.DiskUsed, info.Resources.APDiskUsed)
			} else {
				if info.Resources.DiskHoldMax > 0 && info.Resources.DiskUsed >= info.Resources.DiskHoldMax {
					tasks = fmt.Sprintf("Running: %s %s %s %s | Preparing: %s %s %s %s | DiskUsed: %d >= DiskHoldMax: %d APDiskUsed: %d I am forced into disk constraints mode.", apRunning, p1Running, p2Running, c2Running, apPreparing, p1Preparing, p2Preparing, c2Preparing, info.Resources.DiskUsed, info.Resources.DiskHoldMax, info.Resources.APDiskUsed)
				} else {
					tasks = fmt.Sprintf("Running: %s %s %s %s | Preparing: %s %s %s %s | Capacity: %s %s %s %s | DiskUsed: %d APDiskUsed: %d", apRunning, p1Running, p2Running, c2Running, apPreparing, p1Preparing, p2Preparing, c2Preparing, apTasks, p1Tasks, p2Tasks, c2Tasks, info.Resources.DiskUsed, info.Resources.APDiskUsed)
				}
			}
		}
		out[uuid.UUID(id)] = storiface.WorkerStats{
			Info:       info, //handle.info,
			Enabled: handle.enabled,

			MemUsedMin: handle.active.memUsedMin,
			MemUsedMax: handle.active.memUsedMax,
			GpuUsed:    handle.active.gpuUsed,
			CpuUse:     handle.active.cpuUse,
			TaskTypes:  types,
			Tasks:      tasks,
		}
	}

	return out
}

func (m *Manager) WorkerJobs() map[uuid.UUID][]storiface.WorkerJob {
	out := map[uuid.UUID][]storiface.WorkerJob{}
	calls := map[storiface.CallID]struct{}{}

	for _, t := range m.sched.workTracker.Running() {
		out[uuid.UUID(t.worker)] = append(out[uuid.UUID(t.worker)], t.job)
		calls[t.job.ID] = struct{}{}
	}

	//m.sched.workersLk.RLock()
	//
	//for id, handle := range m.sched.workers {
	//	handle.wndLk.Lock()
	//	for wi, window := range handle.activeWindows {
	//		for _, request := range window.todo {
	//			out[uuid.UUID(id)] = append(out[uuid.UUID(id)], storiface.WorkerJob{
	//				ID:      storiface.UndefCall,
	//				Sector:  request.sector.ID,
	//				Task:    request.taskType,
	//				RunWait: wi + 1,
	//				Start:   request.start,
	//			})
	//		}
	//	}
	//	handle.wndLk.Unlock()
	//}
	//
	//m.sched.workersLk.RUnlock()

	m.workLk.Lock()
	defer m.workLk.Unlock()

	for id, work := range m.callToWork {
		_, found := calls[id]
		if found {
			continue
		}

		var ws WorkState
		if err := m.work.Get(work).Get(&ws); err != nil {
			log.Errorf("WorkerJobs: get work %s: %+v", work, err)
		}

		wait := storiface.RWRetWait
		if _, ok := m.results[work]; ok {
			wait = storiface.RWReturned
		}
		if ws.Status == wsDone {
			wait = storiface.RWRetDone
		}

		out[uuid.UUID{}] = append(out[uuid.UUID{}], storiface.WorkerJob{
			ID:       id,
			Sector:   id.Sector,
			Task:     work.Method,
			RunWait:  wait,
			Start:    time.Unix(ws.StartTime, 0),
			Hostname: ws.WorkerHostname,
		})
	}

	return out
}
