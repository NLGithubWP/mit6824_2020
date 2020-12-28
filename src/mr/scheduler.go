package mr

const (
	MapPhrase = "map"
	ReducePhrase = "reduce"
	StatusFinish  = 1
	StatusStopped  = 2
	StatusBegin  = 3
)


type TaskArgs struct {
}


type TaskReply struct {
	IsFinish  bool
	Phase	  string
	FileName  []string
	// A reasonable naming convention for intermediate files is mr-X-Y,
	// where X is the Map task number, and Y is the reduce task number.
	RTasks    int
	X         int
	Y   	  int
}


func (m *Master) Schedule(args *TaskArgs, reply *TaskReply) error {
	m.Lock()
	defer m.Unlock()

	if len(m.MapFiles) == 0 && len(m.ReduceFiles)==0{
		DPrintf("[Master]: Schedule, no MapFiles and ReduceFiles found! \n")
		if m.isReduceFinish()==true{
			defer m.IsDone.Broadcast()
			reply.IsFinish=true
			DPrintf("[Master]: Finish All, Done return true \n")
			return nil
		}else{
			DPrintf("[Master]: Reduce Task is not finished \n")
			return nil
		}

	}else if len(m.MapFiles) != 0  {
		DPrintf("[Master]: Schedule MapPhrase")

		reply.IsFinish=false
		reply.Phase=MapPhrase
		// a single file to be mapped
		reply.FileName = []string{m.MapFiles[0]}
		reply.RTasks = m.R
		reply.X = m.M - len(m.MapFiles)

		DPrintf("[Master]: Schedule MapPhrase, return res, reply: " +
			"reply.IsFinish: %v, " +
			"reply.Phase: %s, " +
			"reply.RTasks: %d, " +
			"reply.X: %d, " +
			"\n",
			reply.IsFinish, reply.Phase, reply.RTasks, reply.X)

		m.MapTaskStatus[m.MapFiles[0]] = StatusBegin
		go m.mapTaskMonitor(m.MapFiles[0])
		m.MapFiles = m.MapFiles[1:]
		return nil

	}else{
		DPrintf("[Master]: Schedule ReducePhrase")

		if m.isMapFinish()!=true{
			DPrintf("[Master]: Map Task is not finished \n")
			return nil
		}
		reply.IsFinish=false
		reply.Phase=ReducePhrase
		// a list of files to be reduced
		reply.FileName = m.ReduceFiles[0]
		reply.Y = m.R - len(m.ReduceFiles)

		DPrintf("[Master]: Schedule ReducePhrase, return res, reply: " +
			"reply.IsFinish: %v, " +
			"reply.Phase: %s, " +
			"reply.RTasks: %d, " +
			"reply.Y: %d, " +
			"\n",
			reply.IsFinish, reply.Phase, reply.RTasks, reply.Y)

		// use first filename in file name array as index
		m.ReduceTaskStatus[m.ReduceFiles[0][0]] = StatusBegin
		go m.reduceTaskMonitor(m.ReduceFiles[0])
		m.ReduceFiles = m.ReduceFiles[1:]
		return nil
	}
}


type TaskResArgs struct {
	InputFile string
	ReturnFile []string
}


type TaskResReply struct {

}
