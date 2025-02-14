package kvraft

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
)

type Err string

// Field names must start with capital letters,
// otherwise RPC will break.

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
