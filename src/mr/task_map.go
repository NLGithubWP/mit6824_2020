package mr

import "time"

func (m *Master) MapStatusReport(args *TaskResArgs, reply *TaskResReply) error {
	m.Lock()
	if v,ok := m.MapTaskStatus[args.InputFile]; ok && v == StatusBegin{
		m.MapTaskStatus[args.InputFile] = StatusFinish
		for i := 0; i<m.R;i++{
			m.ReduceFiles[i] = append(m.ReduceFiles[i], args.ReturnFile[i])
		}
	}
	m.Unlock()
	return nil
}

func (m *Master) mapTaskMonitor(FileName string)  {
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
		if v, ok := m.MapTaskStatus[FileName]; ok && v==StatusFinish{
			m.Unlock()
			return
		}
		m.Unlock()
	}

	m.Lock()
	m.MapFiles = append(m.MapFiles, FileName)
	m.MapTaskStatus[FileName] = StatusStopped
	m.Unlock()
	return
}


func (m *Master) isMapFinish() bool{

	for _, v:= range m.MapTaskStatus{
		if v==StatusFinish{
			continue
		}else{
			return false
		}
	}
	return true

}