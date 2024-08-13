package mr

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
)

func Test_CreateFile(t *testing.T) {
	tempfile := "./intermedia/mr-1-1.tmp"
	_, err := os.Create(tempfile)
	t.Logf("err:%+v", err)
}

func Test_WorkerProcessReduce(t *testing.T) {
	reduceFunc := func(file string, values []string) string {
		return strconv.Itoa(len(values))
	}
	task := &Task{
		TaskType:   ReduceTask,
		TaskId:     0,
		FileList:   []string{"mr-0-0"},
		NReduce:    5,
		TaskStatus: Idle,
		StartTime:  time.Now(),
	}
	err := processReduce(reduceFunc, task)
	t.Logf("err:%v", err)
}

func Test_WorkerProcessMap(t *testing.T) {
	mapf := func(file string, contents string) []KeyValue {
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}

	task := &Task{
		TaskType:   MapTask,
		TaskId:     0,
		FileList:   []string{"../main/pg-dorian_gray.txt"},
		NReduce:    5,
		TaskStatus: Idle,
		StartTime:  time.Now(),
	}
	err := processMap(mapf, task)
	t.Logf("err:%v", err)
}
