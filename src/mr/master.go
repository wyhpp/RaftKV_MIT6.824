package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//任务状态
type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type TaskType int
const (
	Map TaskType = iota
	Reduce
	Exit
)


var lock sync.Mutex

type Master struct {
	// Your definitions here.
	//reduce
	reduce int
	//文件名
	fileNames[] string
	//中间文件存储位置
	intermediateFile[][] string
	//所有任务信息
	tasksMap map[int]*Task
	//等待执行任务数组
	taskWaitingQ[] Task
	//任务阶段
	MasterPhase TaskType
}

type Task struct {
	//任务类型
	typeName TaskType
	//任务状态
	status TaskStatus
	//任务内容
	content[] byte
	//任务编号
	taskNumber int
	//切分数量
	nReduce int
	//中间文件位置
	intermediateFiles[] string
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
//master分配任务
func (m *Master) AssignTask(args *ExampleArgs, reply *Reply) error {
	//遍历taskmap，查看是否还有未完成的maptask
	flag := true
	if m.MasterPhase == Map {
		lock.Lock()
		for _,task := range m.tasksMap {
			//如果有任务状态为idle的任务存在
			if task.status == TaskStatus(0){
				reply.taskUndo = *task
				fmt.Printf("assign map task %v\n",task.taskNumber)
				//把任务状态改位正在处理中
				task.status = TaskStatus(1)
				flag = false
				break
			}
		}
		lock.Unlock()
	}else if m.MasterPhase == Reduce{
		//分配reduce任务
		//返回任务以及中间文件位置
		lock.Lock()
		for _,task := range m.tasksMap {
			//如果有任务状态为idle的任务存在
			if task.status == TaskStatus(0){
				reply.taskUndo = *task
				fmt.Printf("assign reduce task %v\n",task.taskNumber)
				//把任务状态改位正在处理中
				task.status = TaskStatus(1)
				flag = false
				break
			}
		}
		if flag {
			//没有任务,可以退出
			reply.Y = -1
		}
		lock.Unlock()
	}

	//map任务已经处理完成，进入reduce阶段
	lock.Lock()
	if flag && m.MasterPhase == Map {
		m.MasterPhase = Reduce
		//将任务全部改成reduce任务，状态改为未完成
		//最好重新生成nReduce个task任务
		//清空map
		m.tasksMap = make(map[int]*Task)
		for i := 0; i < m.reduce; i++ {
			task := Task{}
			task.typeName = TaskType(1)
			task.status = TaskStatus(0)
			task.intermediateFiles =m.intermediateFile[task.taskNumber]
			task.taskNumber = i
			//存入map
			m.tasksMap[i] = &task
		}
	}
	lock.Unlock()
	//通过返回值告诉worker线程没有任务时可以退出

	return nil
}

func (m *Master) CompletionHandler(args *Args, reply *Reply) error {
	//如果是map任务完成
	//将中间文件位置加入master中，标记任务完成
	//遍历所有任务查看是否全部完成，若是，开始分配reduce任务
	completedTask := args.taskCompleted
	if completedTask.typeName == Map {
		m.intermediateFile = append(m.intermediateFile,completedTask.intermediateFiles)
		//标记任务完成
		masterTask := m.tasksMap[completedTask.taskNumber]
		masterTask.status = TaskStatus(2)
	}else {
		masterTask := m.tasksMap[completedTask.taskNumber]
		masterTask.status = TaskStatus(2)
		//遍历taskmap，查看是否全部完成
		flag := true
		for _,task := range m.tasksMap {
			if task.status != TaskStatus(2) {
				flag = false
				break
			}
		}
		if flag {
			//将master阶段设为exit
			m.MasterPhase = Exit
		}
	}

	reply.Y = args.taskCompleted.taskNumber
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
	if m.MasterPhase == Exit {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		reduce: nReduce,
		fileNames: files,
		tasksMap: make(map[int]*Task),
		taskWaitingQ: make([]Task,len(files)),
		intermediateFile: make([][]string,nReduce),
		MasterPhase: Map,
	}
	// Your code here.
	//将文件分给不同的worker，直到所有文件被转换为intermediate file
	//将intermediate file 整合后分为nReduce份
	//将分好的文件交给worker，用reduce函数返回计数

	//初始化等待任务数组
	//生成了文件数量个任务数组，初始化任务状态
	for i,task := range m.taskWaitingQ {
		task.status = Idle
		task.typeName = Map
		task.taskNumber = i
		task.nReduce = nReduce
		//加入所有任务信息map
		m.tasksMap[i] = &task
	}

	//读取文件内容，切分任务
	for i, filename := range m.fileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		//把切分的文件内容放入task中
		m.tasksMap[i].content = content
	}

	m.server()
	return &m
}
