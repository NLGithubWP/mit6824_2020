package mr

import "time"



func (m *Master) mapTaskMonitor(splitId int, splitFile string, workerId int)  {
	/*
		The master can't reliably distinguish between crashed workers,
		workers that are alive but have stalled for some reason, and workers
		that are executing but too slowly to be useful. The best you can do is
		have the master wait for some amount of time, and then give up and re-issue
		the task to a different worker. For this lab, have the master wait for ten seconds;
		after that the master should assume the worker has died (of course, it might not have).
	*/
	for i := 1; i <= 10; i++ {
		time.Sleep(time.Second)
		m.Lock()
		if v, ok := m.MapTaskStatus[splitId]; ok && v==StatusFinish{
			DPrintf("[Master]: Worker %d Finish Map Task id %d,file %s \n",
				workerId, splitId, splitFile)

			m.Unlock()
			return
		}
		m.Unlock()
	}

	m.Lock()
	DPrintf("[Master]: Worker %d delay, Map Task %s Failed, ready to be re-assigned\n",
		workerId, splitFile)

	m.MapFiles = append(m.MapFiles, map[int]string{splitId:splitFile})
	m.workers[workerId] = WorkerDelay
	m.Unlock()
	return
}


func (m *Master) isMapFinish() bool{

	// number of task is less than data slices
	if len(m.MapTaskStatus) < m.M{
		return false
	}

	for _, v:= range m.MapTaskStatus{
		if v==StatusFinish{
			continue
		}else{
			return false
		}
	}
	return true

}