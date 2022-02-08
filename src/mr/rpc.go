package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
//master发给worker的命令
type order int
const(
	wait = 1
	exit = -1
)
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Reply struct {
	//要处理的文件名
	Content string
	//任务编号
	Tasknumber int
	//任务类型
	Tasktype TaskType
	//nreduce
	Reduce int
	//
	IntermediateFile []string
	//taskUndo Task
	Y order
}

type Args struct {
	IntermediateFile []string
	Tasknumber int
	Typename TaskType
	//TaskCompleted Task
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
