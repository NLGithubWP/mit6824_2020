package mr

type TaskArgs struct {
	WorkerId int	// workerId, sent by worker
}


type TaskReply struct {
	IsFinish  bool				// is the job finished
	WorkerId  int				// workerId, confirmed by master
	Phase	  string			// map or reduce

	FileName  []string			// generated file names
	// A reasonable naming convention for intermediate files is mr-X-Y,
	// where X is the Map task number, and Y is the reduce task number.
	RTasks    int				// number of reduce tasks
	X         int				// No. map task
	Y   	  int				// No. reduce task
}


type TaskReportArgs struct {
	Phase	  string			// map or reduce
	WorkerId  int				// workerId, confirmed by master
	InputFile string
	IntermediateFiles []string
}


type TaskReportReply struct {
	Accept  bool
}


func (m *Master) Schedule(args *TaskArgs, reply *TaskReply) error {
	m.Lock()
	defer m.Unlock()

	// if worker has a id, return the id, else assign a new id to worker
	if args.WorkerId == WorkerDefaultIndex{
		reply.WorkerId = len(m.workers)
		m.workers = append(m.workers, WorkerNormal)
	}else{
		reply.WorkerId = args.WorkerId
	}

	if len(m.MapFiles) == 0 && len(m.ReduceFiles)==0{
		//DPrintf("[Master]: Schedule, no MapFiles and ReduceFiles found! \n")
		if m.isReduceFinish()==true{
			defer m.IsDone.Broadcast()
			reply.IsFinish=true
			DPrintf("[Master]: Finish All, Done return true \n")
			return nil
		}else{
			//DPrintf("[Master]: Reduce Task is not finished \n")
			return nil
		}

	}else if len(m.MapFiles) != 0  {

		reply.IsFinish=false
		reply.Phase=MapPhrase
		// a single file to be processed in map task
		reply.FileName = []string{m.MapFiles[0]}
		reply.RTasks = m.R
		reply.X = m.M - len(m.MapFiles)

		DPrintf("[Master]: Schedule Map to Worker:" +
			"reply.IsFinish: %v, " +
			"reply.Phase: %s, " +
			"reply.RTasks: %d, " +
			"reply.X: %d, " +
			"reply.WorkerId: %d, " +
			"\n",
			reply.IsFinish, reply.Phase, reply.RTasks, reply.X, reply.WorkerId)

		selectFile := m.MapFiles[0]
		m.MapFiles = m.MapFiles[1:]
		m.workers[reply.WorkerId] = WorkerNormal
		defer func(){
			go m.mapTaskMonitor(selectFile, reply.WorkerId)
		}()
		return nil

	}else{
		if m.isMapFinish()!=true{
			//DPrintf("[Master]: Map Task is not finished... \n")
			return nil
		}
		reply.IsFinish=false
		reply.Phase=ReducePhrase
		// a list of files to be reduced
		reply.FileName = m.ReduceFiles[0]
		reply.Y = m.R - len(m.ReduceFiles)

		DPrintf("[Master]: Schedule Reduce to Worker:" +
			"reply.IsFinish: %v, " +
			"reply.Phase: %s, " +
			"reply.RTasks: %d, " +
			"reply.Y: %d, " +
			"reply.WorkerId: %d, " +
			"\n",
			reply.IsFinish, reply.Phase, reply.RTasks, reply.Y, reply.WorkerId)

		// use first filename in file name array as index
		selectFile := m.ReduceFiles[0]
		m.ReduceFiles = m.ReduceFiles[1:]
		m.workers[reply.WorkerId] = WorkerNormal
		defer func(){
			go m.reduceTaskMonitor(selectFile, reply.WorkerId)
		}()
		return nil
	}

}

func (m *Master) Collect(args *TaskReportArgs, reply *TaskReportReply) error {
	m.Lock()
	defer m.Unlock()
	if m.workers[args.WorkerId] == WorkerDelay{
		reply.Accept = false
		//DPrintf("[Worker]: Map Task Report Status MisMatch: %s, %v \n", args.InputFile, m.MapTaskStatus)
	}else{
		reply.Accept = true

		if args.Phase == MapPhrase{
			m.MapTaskStatus[args.InputFile] = StatusFinish

			for i:=0; i<m.R; i++{
				m.ReduceFiles[i] = append(m.ReduceFiles[i], args.IntermediateFiles[i])
			}
		}else if args.Phase == ReducePhrase{
			m.ReduceTaskStatus[args.InputFile] = StatusFinish
		}else{
			panic("Not Map Or Reduce")
		}

		//DPrintf("[Worker]: Map Task Report Status Match: %s \n", v)
	}
	return nil
}