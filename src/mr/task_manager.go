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
	SplitId int
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
		/*
		7. When all map tasks and reduce tasks have been
		completed, the master wakes up the user program.
		At this point, the MapReduce call in the user program
		returns back to the user code.
		 */

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

		var splitId int
		var splitFile string
		selectFile := m.MapFiles[0]
		for k, v := range selectFile{
			splitId = k
			splitFile = v
		}
		if len(splitFile) == 0{
			DPrintf("[Master]: No file found" +
				"MapFiles: %d\n", m.MapFiles)

			panic("No file found")
		}
		reply.IsFinish=false
		reply.Phase=MapPhrase
		reply.RTasks = m.R
		reply.X = splitId
		reply.FileName = []string{splitFile}

		DPrintf("[Master]: Schedule Map to Worker:-->" +
			"reply.WorkerId: %d, " +
			"reply.X: %d, " +
			"reply.FileName: %s, " +
			"reply.IsFinish: %v, " +
			"reply.Phase: %s, " +
			"reply.RTasks: %d, " +
			"\n",
			reply.WorkerId, reply.X, reply.FileName, reply.IsFinish, reply.Phase, reply.RTasks)

		m.MapFiles = m.MapFiles[1:]
		m.workers[reply.WorkerId] = WorkerNormal
		defer func(){
			go m.mapTaskMonitor(splitId, splitFile, reply.WorkerId)
		}()
		return nil

	}else{
		if m.isMapFinish()!=true{
			//DPrintf("[Master]: Map Task is not finished... \n")
			return nil
		}

		var reduceId int
		var reduceFiles []string
		selectFile := m.ReduceFiles[0]
		for k, v := range selectFile{
			reduceId = k
			reduceFiles = v
		}

		reply.IsFinish=false
		reply.Phase=ReducePhrase
		reply.Y = reduceId
		reply.FileName = reduceFiles

		DPrintf("[Master]: Schedule Reduce to Worker:-->" +
			"reply.WorkerId: %d, " +
			"reply.Y: %d, " +
			"reply.FileName: %s, " +
			"reply.IsFinish: %v, " +
			"reply.Phase: %s, " +
			"reply.RTasks: %d, " +
			"\n",
			reply.WorkerId, reply.Y, reply.FileName, reply.IsFinish, reply.Phase, reply.RTasks)

		// use first filename in file name array as index
		m.ReduceFiles = m.ReduceFiles[1:]
		m.workers[reply.WorkerId] = WorkerNormal
		defer func(){
			go m.reduceTaskMonitor(reduceId, reduceFiles, reply.WorkerId)
		}()
		return nil
	}

}

func (m *Master) Collect(args *TaskReportArgs, reply *TaskReportReply) error {
	m.Lock()
	defer m.Unlock()
	if m.workers[args.WorkerId] == WorkerDelay{
		reply.Accept = false

		DPrintf("[Master]: Refuse to accept, as worker %d is delay: \n", args.WorkerId)

	}else{
		reply.Accept = true

		if args.Phase == MapPhrase{
			m.MapTaskStatus[args.SplitId] = StatusFinish

			for i:=0; i<m.R; i++{

				if m.ReduceFiles[i] == nil{
					panic("ReduceFiles init error")
				}
				m.ReduceFiles[i][i] = append(m.ReduceFiles[i][i], args.IntermediateFiles[i])
			}
		}else if args.Phase == ReducePhrase{
			m.ReduceTaskStatus[args.SplitId] = StatusFinish
		}else{
			panic("Not Map Or Reduce")
		}

		DPrintf("[Master]: Agree to accept, as worker %d is finished: \n", args.WorkerId)
	}
	return nil
}