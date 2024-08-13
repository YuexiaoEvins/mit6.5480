package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TaskTimeout = 10 * time.Second

type processStage int

const (
	MapStage processStage = iota
	ReduceStage
	AllDoneStage
)

type Coordinator struct {
	// Your definitions here.
	mu             sync.RWMutex
	mapTaskList    []*Task
	reduceTaskList []*Task
	nReduce        int
	mapCounter     int
	reduceCounter  int
	stage          processStage
	exitTimer      time.Timer
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) NotifyTask(req *NotifyTaskRequest, reply *NotifyTaskReply) error {
	reportTask := req.Task
	if reportTask == nil {
		return errors.New("nil report task")
	}
	if reportTask.TaskStatus == Failed {
		fmt.Printf("receive failed task:%v\n", req.Task)
		return nil
	}

	if time.Since(reportTask.StartTime) >= TaskTimeout {
		//ignore timeout task
		fmt.Printf("receive timeout task:%v, ingore \n", req.Task)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch reportTask.TaskType {
	case MapTask:
		fmt.Printf("req task:%+v inter:%+v\n", req.Task, req.IntermediaFile)
		c.mapTaskList[req.Task.TaskId].TaskStatus = Complete
		for ind, files := range req.IntermediaFile {
			c.reduceTaskList[ind].FileList = append(c.reduceTaskList[ind].FileList, files)
		}
		c.mapCounter++
		if c.mapCounter >= len(c.mapTaskList) {
			fmt.Printf("start reduce stage\n")
			c.stage = ReduceStage
		}
		return nil
	case ReduceTask:
		fmt.Printf("receive reduce task:%+v inter:%+v\n", req.Task, req.IntermediaFile)
		c.reduceTaskList[req.Task.TaskId].TaskStatus = Complete
		c.reduceCounter++
		if c.reduceCounter >= len(c.reduceTaskList) {
			fmt.Printf("all reduce task finished\n")
			c.stage = AllDoneStage
			c.exitTimer = *time.NewTimer(10 * time.Second)
			go func() {
				for {
					select {
					case <-c.exitTimer.C:
						os.Exit(0)
					}
				}
			}()
		}
		return nil
	}

	return nil
}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var taskList []*Task

	if c.stage == AllDoneStage {
		c.exitTimer.Reset(10 * time.Second)
		doneTask := &Task{
			TaskType: ExitTask,
		}
		reply.Task = doneTask
		return nil
	}

	if c.stage == MapStage {
		taskList = c.mapTaskList
	} else {
		if c.stage == ReduceStage {
			taskList = c.reduceTaskList
		}
	}

	for _, task := range taskList {
		if task.TaskStatus == Complete {
			continue
		}

		if task.TaskStatus == Idle {
			task.StartTime = time.Now()
			task.TaskStatus = InProcess
			reply.Task = task
			return nil
		}

		if task.TaskStatus == InProcess {
			if time.Since(task.StartTime) >= TaskTimeout {
				//return time out task
				task.StartTime = time.Now()
				reply.Task = task
				return nil
			}
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	//todo fix me
	return c.stage == AllDoneStage
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		stage:   MapStage,
	}
	// Your code here.
	var taskList []*Task
	for ind, file := range files {
		taskList = append(taskList, &Task{
			TaskType:   MapTask,
			TaskId:     ind,
			FileList:   []string{file},
			NReduce:    nReduce,
			TaskStatus: Idle,
		})
	}
	c.mapTaskList = taskList

	reduceTaskList := make([]*Task, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTaskList = append(reduceTaskList, &Task{
			TaskType:   ReduceTask,
			TaskId:     i,
			FileList:   []string{},
			NReduce:    nReduce,
			TaskStatus: Idle,
		})
	}
	c.reduceTaskList = reduceTaskList

	c.server()
	return &c
}
