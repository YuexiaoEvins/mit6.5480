package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      uint32
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key      string
	ClientId int64
	Seq      uint32
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
