package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getIntermediaFileName(taskId, reduceId int, ts int64) string {
	return fmt.Sprintf("mr-%d-%d.tmp%d", taskId, reduceId, ts)
}
func getMapOutputFileName(taskId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", taskId, reduceId)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := GetTask()
		if task == nil {
			fmt.Printf("Nil task!\n")
			time.Sleep(1 * time.Second)
			continue
		}
		fmt.Printf("worker receive task:%+v\n", task)
		switch task.TaskType {
		case MapTask:
			err := processMap(mapf, task)
			if err != nil {
				NotifyFailedTask(task)
				fmt.Printf("process map error:%v\n", err)
				continue
			}
		case ReduceTask:
			err := processReduce(reducef, task)
			if err != nil {
				NotifyFailedTask(task)
				fmt.Printf("process map error:%v\n", err)
				continue
			}
		case ExitTask:
			return
		default:
			fmt.Printf("Unknown task, sleep 1s\n")
			time.Sleep(1 * time.Second)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func processReduce(reducef func(string, []string) string, task *Task) error {
	var kva []KeyValue
	for _, file := range task.FileList {
		fd, err := os.Open(file)
		if err != nil {
			fmt.Printf("open file failed: %v\n", err)
			return err
		}

		dec := json.NewDecoder(fd)
		for {
			var kv []KeyValue
			if dErr := dec.Decode(&kv); dErr != nil {
				break
			}
			kva = append(kva, kv...)
		}
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	tmpFileName := fmt.Sprintf("mr-out-%d.tmp%v", task.TaskId, task.StartTime)
	tmpFd, err := os.Create(tmpFileName)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmpFd.Close()
		_ = os.Remove(tmpFileName)
	}()

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		reduceOutput := reducef(kva[i].Key, values)
		_, _ = tmpFd.WriteString(fmt.Sprintf("%v %v\n", kva[i].Key, reduceOutput))
		i = j
	}

	err = os.Rename(tmpFileName, strings.TrimSuffix(tmpFileName, fmt.Sprintf(".tmp%v", task.StartTime)))
	if err != nil {
		return err
	}

	NotifyReduceTask(task)
	return nil
}

func processMap(mapf func(string, string) []KeyValue, task *Task) error {
	//todo should implement defer clean up function

	kva := make([][]KeyValue, task.NReduce)
	for _, file := range task.FileList {
		content, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("mapping open file failed: %v\n", err)
			return err
		}

		kvList := mapf(file, string(content))
		for _, kv := range kvList {
			reduceIndex := ihash(kv.Key) % task.NReduce
			kva[reduceIndex] = append(kva[reduceIndex], kv)
		}
	}

	intermediaFileList := make([]*os.File, 0, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		fileName := getIntermediaFileName(task.TaskId, i, task.StartTime.Unix())
		file, err := os.Create(fileName)
		if err != nil {
			fmt.Printf("mapping create file:%s failed: %v\n", fileName, err)
			return err
		}
		intermediaFileList = append(intermediaFileList, file)

		encoder := json.NewEncoder(file)
		err = encoder.Encode(kva[i])
		if err != nil {
			fmt.Printf("mapping encode file:%s failed: %v\n", fileName, err)
			return err
		}
	}

	defer func() {
		for _, file := range intermediaFileList {
			_ = file.Close()
			_ = os.Remove(file.Name())
		}
	}()

	intermediaFileMap := make(map[int]string, task.NReduce)
	for ind, interFile := range intermediaFileList {
		outputName := strings.TrimSuffix(interFile.Name(), fmt.Sprintf(".tmp%d", task.StartTime.Unix()))
		if rErr := os.Rename(interFile.Name(), outputName); rErr != nil {
			//maybe exist
			lastFileName := strings.TrimSuffix(getIntermediaFileName(task.TaskId, task.NReduce-1, task.StartTime.Unix()),
				fmt.Sprintf(".tmp%d", task.StartTime.Unix()))
			if _, err := os.Stat(lastFileName); errors.Is(err, os.ErrNotExist) {
				_ = os.Remove(outputName)
				_ = os.Rename(interFile.Name(), outputName)
			}
		}
		intermediaFileMap[ind] = outputName
	}

	NotifyMapTask(task, intermediaFileMap)

	return nil
}

func NotifyFailedTask(task *Task) {
	task.TaskStatus = Failed
	req := &NotifyTaskRequest{
		Task: task,
	}
	reply := &NotifyTaskReply{}
	ok := call(RpcNotifyTask, req, reply)
	if !ok {
		fmt.Printf("call %s args:%v reply:%v failed!\n", RpcNotifyTask, req, reply)
	}
}

func NotifyReduceTask(task *Task) {
	req := &NotifyTaskRequest{
		Task: task,
	}
	reply := &NotifyTaskReply{}
	ok := call(RpcNotifyTask, req, reply)
	if !ok {
		fmt.Printf("call %s args:%v reply:%v failed!\n", RpcNotifyTask, req, reply)
	}
}
func NotifyMapTask(task *Task, intermediaFile map[int]string) {
	req := &NotifyTaskRequest{
		Task:           task,
		IntermediaFile: intermediaFile,
	}
	reply := &NotifyTaskReply{}
	ok := call(RpcNotifyTask, req, reply)
	if !ok {
		fmt.Printf("call %s args:%v reply:%v failed!\n", RpcNotifyTask, req, reply)
	}
}

func GetTask() *Task {
	args := &GetTaskRequest{}
	reply := &GetTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call(RpcGetTask, args, reply)
	if !ok {
		fmt.Printf("call %s args:%v reply:%v failed!\n", RpcGetTask, args, reply)
		return nil
	}
	return reply.Task
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
