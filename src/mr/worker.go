package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"



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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	//循环获取任务
	for true {
		task,Order := GetTask()
		if Order == wait {
			time.Sleep(time.Second)
			continue
		}
		if Order == exit{
			//已经没有任务，退出
			fmt.Println("worker退出")
			break
		}
		if task.typeName == Map{
			MapProcess(&task,mapf)
		}else if task.typeName == Reduce {
			ReduceProcess(&task,reducef)
		}
		//任务完成
		TaskComplete(&task)
	}
	//fmt.Println("worker 退出")
}

//map任务处理
func MapProcess(task *Task,mapf func(string, string) []KeyValue)  {
	task.status = TaskStatus(1)
	filename := task.content
	intermediate := []KeyValue{}
	//读取文件内容
	file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open map %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read map %v", filename)
		}
		file.Close()

	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	//输出intermediate到文件
	//将文件切分成r份
	//map输出切分时，buffer的第一维是nReduce个,相同的key值根据hash函数会放入相同的槽中，
	//相应的，reduce任务需要nReduce个，各自去取一个槽里的文件，那么所有相同key值的都会被取到，不用担心统计不全
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
	fmt.Printf("intermediate file stored ,name is %v\n",oname)
	return oname
}

func ReduceProcess(task *Task,reducef func(string, []string) string)  {
	//按key值排序然后计数
	//读取文件内容
	intermediateContents := []KeyValue{}
	//fmt.Println("中间文件",task.intermediateFiles)
	for i := range task.intermediateFiles {
		kv := ReadFile(task.intermediateFiles[i])
		intermediateContents = append(intermediateContents,kv...)
	}
	//按key排序
	sort.Sort(ByKey(intermediateContents))

	oname := "mr-out-" + strconv.Itoa(task.taskNumber)
	ofile, _ := ioutil.TempFile("./", "mr-out-tmp*") //先写道tmp文件中
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	//fmt.Println("中间内容",intermediateContents[:10])
	for i < len(intermediateContents) {
		j := i + 1
		for j < len(intermediateContents) && intermediateContents[j].Key == intermediateContents[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateContents[k].Value)
		}
		output := reducef(intermediateContents[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediateContents[i].Key, output)

		i = j
	}
	ofile.Close()

	os.Rename(ofile.Name(), oname) //Reduce操作，完成之后修改名字

	//删除中间文件
	for _, inputFileName := range task.intermediateFiles {
		os.Remove(inputFileName)
	}

}

func ReadFile(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open reduce %v", filename)
	}
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	//fmt.Printf("内容是 %v\n",kva[:5])
	return kva
}
//map或reduce处理完成
func TaskComplete(task *Task)  {
	//任务处理完成后通知master
	CallCompleted(*task)
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//向master请求任务
func GetTask() (Task,order){

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.AssignTask", &args, &reply)
	//处理reply中的Task
	task := Task{
		taskNumber: reply.Tasknumber,
		content: reply.Content,
		typeName: reply.Tasktype,
		nReduce: reply.Reduce,
		intermediateFiles: reply.IntermediateFile,
	}


	// reply.Y should be 100.
	//fmt.Printf("gettask %v\n", reply.Tasknumber)

	return task,reply.Y
}

func CallCompleted(task Task){

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	task.status = TaskStatus(2)
	args.IntermediateFile = task.intermediateFiles
	args.Tasknumber = task.taskNumber
	args.Typename = task.typeName

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.CompletionHandler", &args, &reply)

	//返回task状态

	// reply.Y should be 100.
	//fmt.Printf("task completed,task number is %v\n", reply.Y)

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
