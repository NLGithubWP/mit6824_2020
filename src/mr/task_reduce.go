package mr

import "time"

func (m *Master) ReduceStatusReport(args *TaskResArgs, reply *TaskResReply) error {
	m.Lock()
	if v,ok:=m.ReduceTaskStatus[args.InputFile]; ok && v == StatusBegin{
		m.ReduceTaskStatus[args.InputFile] = StatusFinish
	}
	m.Unlock()

	return nil
}


func (m *Master) reduceTaskMonitor(FileName []string)  {

	for i := 1; i <= 10; i++ {
		time.Sleep(time.Second)
		m.Lock()
		if v, ok := m.MapTaskStatus[FileName[0]]; ok && v==StatusFinish{
			m.Unlock()
			return
		}
		m.Unlock()
	}

	m.Lock()
	m.ReduceFiles = append(m.ReduceFiles, FileName)
	m.ReduceTaskStatus[FileName[0]] = StatusStopped
	m.Unlock()
	return
}

func (m *Master) isReduceFinish() bool{


	// if the reduce task have not begin yet
	if len(m.ReduceTaskStatus) == 0{
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
