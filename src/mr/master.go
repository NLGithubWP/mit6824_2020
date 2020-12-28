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
	M  int     							// number of map tasks
	R  int	   							// number of reduce tasks

	MapFiles []string    				// data splits
	ReduceFiles [][]string				// each element have value of the same key
	MapTaskStatus map[string]int		// status of each map task
	ReduceTaskStatus map[string]int		// status of each reduce task

	IsDone  *sync.Cond					// if all task are done

	workers   []int						// each element is the status of worker
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

	m.workers = make([]int, 0)

	m.server()
	return &m
}
