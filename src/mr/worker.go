package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf MapF,
	reducef ReduceF,
	) {

	WorkerId := WorkerDefaultIndex

	for {
		args := TaskArgs{}
		reply := TaskReply{}
		args.WorkerId = WorkerId

		err := call("Master.Schedule", &args, &reply)
		if err != true {
			DPrintf("[Worker]: Call Master Schedule error\n")
		}

		// if master return isFinish, break the loop
		if reply.IsFinish==true{
			DPrintf("[Worker]: TaskDone. quit\n")
			return
		}
		WorkerId = reply.WorkerId
		switch reply.Phase {
		case MapPhrase:
			execMap(&reply, mapf)
		case ReducePhrase:
			execReduce(&reply, reducef)
		}
		/*
		Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished.
		One possibility is for workers to periodically ask the master for work, sleeping with time.Sleep()
		between each request. Another possibility is for the relevant RPC handler in the master to have a
		loop that waits, either with time.Sleep() or sync.Cond. Go runs the handler for each RPC in its own
		thread, so the fact that one handler is waiting won't prevent the master from processing other RPCs.
		 */
		time.Sleep(time.Millisecond*10)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", rpcname, err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func execMap(reply *TaskReply, mapf MapF){
	//DPrintf("[Worker]: Assigned Map, partitionF, x %d and nReduce %d, \n", reply.X, reply.RTasks)

	filename := reply.FileName[0]
	content := readFile(filename)
	kva := mapf(filename, content)

	tmpFiles, realFiles := partitionF(kva, reply.X, reply.RTasks)

	xargs := TaskReportArgs{}

	xargs.Phase = reply.Phase
	xargs.WorkerId = reply.WorkerId
	xargs.InputFile = filename
	xargs.IntermediateFiles = realFiles
	// return the real nameFiles,
	xreply := TaskReportReply{}
	err := call("Master.Collect", &xargs, &xreply)
	if err != true {
		DPrintf("[Worker]: After Reduce, Call Master.Collect error\n")
	}

	//DPrintf("[Worker]: reply.Accept: %s \n", xreply.Accept)
	if xreply.Accept == true{
		for i, v := range tmpFiles{
			//DPrintf("[Worker]: Renaming %s to %s\n", v, realFiles[i])
			err2 := os.Rename(v, realFiles[i])
			if err2 != nil {
				DPrintf("[Worker]: Rename error: %s\n", err2)
				panic(err2)
			}
		}
	}else{
		DPrintf("[Worker]: Not Renaming %v \n", tmpFiles)
	}

}

func partitionF(kva []KeyValue, x, nReduce int) (tmpFiles, realFiles []string) {
	/*
		The map part of your worker can use the ihash(key) function (in worker.go) to pick the reduce task for a
		given key.

		You can steal some code from mrsequential.go for reading Map input files, for sorting
		intermedate key/value pairs between the Map and Reduce, and for storing Reduce output in files.

	*/
	intermediate := make([][]KeyValue, nReduce)

	for _, kv := range kva{

		part := ihash(kv.Key) % nReduce
		intermediate[part] = append(intermediate[part], kv)
	}
	for i, v := range intermediate{
		sort.Sort(ByKey(v))

		f, e :=ioutil.TempFile("./", "tmpFile---")
		if e!=nil{
			panic(e)
		}
		tmpName := f.Name()
		realName := fmt.Sprintf(BaseDir+"mr-%d-%d", x, i)
		toJsonFile(tmpName, v)
		tmpFiles = append(tmpFiles, tmpName)
		realFiles = append(realFiles, realName)
	}
	return tmpFiles, realFiles
}

func execReduce(reply *TaskReply,  reducef ReduceF){
	//DPrintf("[Worker]: Assigned Reduce \n")
	filenames := reply.FileName

	var intermediate []KeyValue
	for _, fname := range filenames{
		kva := Json2String(fname)
		intermediate = append(intermediate, kva...)
	}
	//DPrintf("[Worker]: Reduce Args: %v \n", intermediate)
	tmpFiles, realFiles := reduceHelper(intermediate, reducef, reply.Y)

	xargs := TaskReportArgs{}
	xargs.Phase = reply.Phase
	xargs.WorkerId = reply.WorkerId
	xargs.InputFile = filenames[0]
	xreply := TaskReportReply{}
	err := call("Master.Collect", &xargs, &xreply)
	if err != true {
		DPrintf("[Worker]: After Reduce, Call Master Collect error\n")
	}

	//DPrintf("[Worker]: reply.Accept: %s \n", xreply.Accept)
	if xreply.Accept == true{
		for i, v := range tmpFiles{
			//DPrintf("[Worker]: Renaming %s to %s\n", v, realFiles[i])
			err2 := os.Rename(v, realFiles[i])
			if err2 != nil {
				DPrintf("[Worker]: Rename error: %s\n", err2)
				panic(err2)
			}
		}
	}else{
		DPrintf("[Worker]: Not Renaming %v \n", tmpFiles)
	}

}

func reduceHelper(intermediate []KeyValue, reducef ReduceF, y int) (tmpFiles, realFiles []string) {

	sort.Sort(ByKey(intermediate))

	ofile, e :=ioutil.TempFile("./", "outf---")
	if e!=nil{
		panic(e)
	}
	tmpName := ofile.Name()
	realName := fmt.Sprintf(BaseDir+"mr-out-%d",y)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	tmpFiles = append(tmpFiles, tmpName)
	realFiles = append(realFiles, realName)
	return tmpFiles, realFiles
}