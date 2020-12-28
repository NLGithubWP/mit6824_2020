package mr

import (
	"fmt"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		args := TaskArgs{}
		reply := TaskReply{}
		err := call("Master.Schedule", &args, &reply)
		if err != true {
			DPrintf("[Worker]: Call Master Schedule error\n")
		}

		//DPrintf("[Worker]: reply infos, reply: " +
		//	"reply.IsFinish: %v, " +
		//	"reply.Phase: %s, " +
		//	"reply.nReduce: %s, " +
		//	"reply.X: %s, " +
		//	"\n",
		//	reply.IsFinish, reply.Phase, reply.RTasks, reply.X)

		// if master return isFinish, break the loop
		if reply.IsFinish==true{
			DPrintf("[Worker]: TaskDone. quit\n")
			return
		}
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
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
		log.Fatal("dialing:", err)
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
	DPrintf("[Worker]: assigned Map, partitionF, x %d and nReduce %d, \n", reply.X, reply.RTasks)

	filename := reply.FileName[0]
	content := readFile(filename)
	kva := mapf(filename, content)

	files := partitionF(kva, reply.X, reply.RTasks)

	xargs := TaskResArgs{}
	xargs.InputFile = filename
	xargs.ReturnFile = files
	xreply := TaskResReply{}
	err := call("Master.MapStatusReport", &xargs, &xreply)
	if err != true {
		DPrintf("[Worker]: Call MapStatusReport error\n")
	}
}

func partitionF(kva []KeyValue, x, nReduce int) (files []string) {
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
		oname := fmt.Sprintf(BaseDir+"mr-%d-%d", x, i)
		toJsonFile(oname, v)
		files = append(files, oname)
	}
	return files
}

func execReduce(reply *TaskReply,  reducef ReduceF){
	DPrintf("[Worker]: assigned Reduce \n")
	filenames := reply.FileName

	var intermediate []KeyValue
	for _, fname := range filenames{
		kva := Json2String(fname)
		intermediate = append(intermediate, kva...)
	}
	reduceHelper(intermediate, reducef, reply.Y)

	xargs := TaskResArgs{}
	xargs.InputFile = filenames[0]
	xreply := TaskResReply{}
	err := call("Master.ReduceStatusReport", &xargs, &xreply)
	if err != true {
		DPrintf("[Worker]: Call Master ReduceStatusReport error\n")
	}
}

func reduceHelper(intermediate []KeyValue, reducef ReduceF, y int){

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf(BaseDir+"mr-out-%d",y)
	ofile, _ := os.Create(oname)

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
}