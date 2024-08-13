package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

const (
	RpcGetTask    = "Coordinator.GetTask"
	RpcNotifyTask = "Coordinator.NotifyTask"
)

type WorkerState int

const (
	idle      WorkerState = iota
	inProcess             = iota
	complete              = iota
)

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProcess
	Complete
	Failed
)

type Task struct {
	TaskType   TaskType
	TaskId     int
	FileList   []string
	NReduce    int
	TaskStatus TaskStatus
	StartTime  time.Time
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskRequest struct {
}

type GetTaskReply struct {
	Task *Task
}

type NotifyTaskRequest struct {
	Task           *Task
	IntermediaFile map[int]string
}

type NotifyTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
