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

	MapFiles []map[int]string    		// data splits eg: [[id1: file1], [id2: file2],]
	ReduceFiles []map[int][]string		// [ [id1: [f1,f2,f3]], [id2: [f1,f2,f3]] ]
	MapTaskStatus map[int]int			// status of each map task
	ReduceTaskStatus map[int]int		// status of each reduce task

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

	/*
	1. The MapReduce library in the user program first
	splits the input files into M pieces of typically 16
	megabytes to 64 megabytes (MB) per piece (controllable
	by the user via an optional parameter). It
	then starts up many copies of the program on a cluster
	of machines.
	 */

	/*
	There areM map tasks and R reduce
	tasks to assign. The master picks idle workers and
	assigns each one a map task or a reduce task.
	 */
	m.M = len(files)
	m.R = nReduce

	m.MapFiles = make([]map[int]string, len(files))

	for i, f:= range files{
		m.MapFiles[i] = map[int]string{i:f}
	}

	m.ReduceFiles = make([]map[int][]string, nReduce)

	for i, _ := range m.ReduceFiles{
		m.ReduceFiles[i] = make(map[int][]string)
	}

	m.MapTaskStatus = make(map[int]int)
	m.ReduceTaskStatus = make(map[int]int)

	m.IsDone = sync.NewCond(&m)

	m.workers = make([]int, 0)

	m.server()
	return &m
}
