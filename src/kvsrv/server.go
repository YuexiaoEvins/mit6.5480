package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type History struct {
	Seq   uint32
	Value string
}

type KVServer struct {
	mu          sync.Mutex
	cache       map[string]string
	callHistory map[int64]*History

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ex := kv.callHistory[args.ClientId]; !ex {
		reply.Value = kv.cache[args.Key]
		return
	}

	history := kv.callHistory[args.ClientId]
	if history.Seq == args.Seq {
		reply.Value = history.Value
		return
	}

	reply.Value = kv.cache[args.Key]
	history.Seq = args.Seq
	history.Value = kv.cache[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.cache[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ex := kv.callHistory[args.ClientId]; !ex {
		kv.callHistory[args.ClientId] = &History{}
	}

	ht := kv.callHistory[args.ClientId]
	if ht.Seq == args.Seq {
		reply.Value = ht.Value
		return
	}

	ht.Seq = args.Seq
	ht.Value = kv.cache[args.Key]
	reply.Value = kv.cache[args.Key]
	kv.cache[args.Key] = kv.cache[args.Key] + args.Value
}

func StartKVServer() *KVServer {
	// You may need initialization code here.
	kv := &KVServer{
		cache:       make(map[string]string),
		callHistory: make(map[int64]*History),
	}

	return kv
}
