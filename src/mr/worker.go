package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"


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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	task := CallForTask()

	task.status = TaskStatus(1)
	filename := ""
	intermediate := []KeyValue{}
	kva := mapf(filename, string(task.content))
	intermediate = append(intermediate, kva...)
	//输出intermediate到文件
	//将文件切分成r份
	buffer := make([][]KeyValue, task.nReduce)
	for _, inter := range intermediate {
		slot := ihash(inter.Key) % task.nReduce
		buffer[slot] = append(buffer[slot], inter)
	}

	//将文件存储到本地
	fileOutput := make([]string,0)
	for i := range buffer {
		fileOutput = append(fileOutput,StoreToLocal(task.taskNumber,i,buffer[i]))
	}
	//将文件名字(位置)返回给master
	task.intermediateFiles = fileOutput
	
}
//中间文件存储到磁盘
func StoreToLocal(taskNumber int,reduceNumber int ,buffer []KeyValue) string {
	oname := "mr-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(reduceNumber)
	file, _ := os.Create(oname)
	enc := json.NewEncoder(file)
	for _, kv := range buffer {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("store intermediate file error", err)
		}
	}
	file.Close()
	return oname
}
//map或reduce处理完成
func TaskComplete(task Task)  {
	//任务处理完成后通知master
	if task.typeName == TaskType(0) {
		//如果是map任务
		CallCompleted(task)
	}else if task.typeName == TaskType(1) {
		//如果是ruduce任务
	}
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallForTask() Task{

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	//处理reply中的Task
	task := reply.taskUndo

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)

	return task
}

func CallCompleted(task Task){

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	//返回task状态

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
