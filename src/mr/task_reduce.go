package mr

import "time"

func (m *Master) reduceTaskMonitor(reduceId int, reduceFiles []string, workerId int)  {

	for i := 1; i <= 10; i++ {
		time.Sleep(time.Second)
		m.Lock()
		if v, ok := m.ReduceTaskStatus[reduceId]; ok && v==StatusFinish{
			DPrintf("[Worker]: one Reduce Task Finished\n")
			m.Unlock()
			return
		}
		m.Unlock()
	}

	m.Lock()
	DPrintf("[Worker]: one Reduce Task Failed, ready to be re-assigned\n")
	m.ReduceFiles = append(m.ReduceFiles, map[int][]string{reduceId:reduceFiles})
	m.workers[workerId] = WorkerDelay
	m.Unlock()
	return
}

func (m *Master) isReduceFinish() bool{

	// if the reduce task have not begin yet
	if len(m.ReduceTaskStatus) < m.R{
		return false
	}

	for _, v:= range m.ReduceTaskStatus{
		if v==StatusFinish{
			continue
		}else{
			return false
		}
	}
	return true
}
