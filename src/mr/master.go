package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


type Master struct {
	// Your definitions here.
	sync.Mutex
	M  int
	R  int

	MapFiles []string
	ReduceFiles [][]string
	MapTaskStatus map[string]int
	ReduceTaskStatus map[string]int

	IsDone  *sync.Cond

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	m.Lock()
	m.IsDone.Wait()
	ret = true
	m.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	DPrintf("[Master]: MakeMaster..., file length: %d, eg: %s\n", len(files), files[0])
	// Your code here.

	m.M = len(files)
	m.R = nReduce

	m.MapFiles = files
	m.ReduceFiles = make([][]string, nReduce)

	m.MapTaskStatus = make(map[string]int)
	m.ReduceTaskStatus = make(map[string]int)

	m.IsDone = sync.NewCond(&m)

	m.server()
	return &m
}
