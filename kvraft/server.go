package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/anirudhsudhir/mit_dist_sys_labs/labgob"
	"github.com/anirudhsudhir/mit_dist_sys_labs/labrpc"
	"github.com/anirudhsudhir/mit_dist_sys_labs/raft"
)

type OpCommand string

const (
	PutKey    OpCommand = "PutKey"
	AppendKey OpCommand = "AppendKey"
	GetKey    OpCommand = "GetKey"
	DeleteKey OpCommand = "DeleteKey"
)

type Op struct {
	Cmd   OpCommand
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh *chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	kvStore      *sync.Map
	leaderExists atomic.Bool

	debugStartTime time.Time
}

func (kv *KVServer) KvNodeState() string {
	_, isLeader := kv.rf.GetState()
	if isLeader {
		return "Leader"
	} else {
		return "NotLeader"
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Received a Get Key RPC")

	op := Op{
		GetKey,
		args.Key,
		"",
	}

	if !kv.leaderExists.Load() {
		kv.mu.Unlock()
		time.Sleep(time.Second)
		kv.mu.Lock()
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Starting agreement on new Operation = %+v", op)

	logIndex, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Node is not leader")
		reply.Value = ""
		reply.Err = ErrWrongLeader
		return
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Waiting for entry from Raft")
	logEntry := <-*kv.applyCh
	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Received entry from Raft, entry = %+v", logEntry)

	kv.leaderExists.CompareAndSwap(false, true)
	cmd := logEntry.Command.(Op)

	if logEntry.CommandIndex != logIndex || cmd.Cmd != GetKey || cmd.Key != args.Key {
		DebugNode(kv.debugStartTime, dInconsistentLogEntry, kv.me, kv.KvNodeState(), "Inconsistent Log Entry at Index = %d, Command = %+v", logIndex, logEntry)
		panic(dInconsistentLogEntry)
	}

	val, ok := kv.kvStore.Load(args.Key)
	if !ok {
		reply.Value = ""
		reply.Err = ErrNoKey
		return
	}

	reply.Value = val.(string)
	reply.Err = OK

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Applied entry to state machine")
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DebugNode(kv.debugStartTime, dPutKeyNode, kv.me, kv.KvNodeState(), "Received a Put Key RPC")

	op := Op{
		PutKey,
		args.Key,
		args.Value,
	}

	logIndex, _, isLeader := kv.rf.Start(op)

	if !kv.leaderExists.Load() {
		kv.mu.Unlock()
		time.Sleep(time.Second)
		kv.mu.Lock()
	}

	if !isLeader {
		DebugNode(kv.debugStartTime, dPutKeyNode, kv.me, kv.KvNodeState(), "Node is not leader")
		reply.Err = ErrWrongLeader
		return
	}

	logEntry := <-*kv.applyCh
	kv.leaderExists.CompareAndSwap(false, true)
	cmd := logEntry.Command.(Op)

	DebugNode(kv.debugStartTime, dPutKeyNode, kv.me, kv.KvNodeState(), "Received entry from Raft, entry = %+v", logEntry)

	if logEntry.CommandIndex != logIndex || cmd.Cmd != PutKey || cmd.Key != args.Key {
		DebugNode(kv.debugStartTime, dInconsistentLogEntry, kv.me, kv.KvNodeState(), "Inconsistent Log Entry at Index = %d, Command = %+v", logIndex, logEntry)
		panic(dInconsistentLogEntry)
	}

	kv.kvStore.Store(args.Key, args.Value)
	DebugNode(kv.debugStartTime, dPutKeyNode, kv.me, kv.KvNodeState(), "Applied entry to state machine")

	reply.Err = OK
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		AppendKey,
		args.Key,
		args.Value,
	}

	logIndex, _, isLeader := kv.rf.Start(op)

	if !kv.leaderExists.Load() {
		time.Sleep(time.Second)
	}

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	logEntry := <-*kv.applyCh
	kv.leaderExists.CompareAndSwap(false, true)
	cmd := logEntry.Command.(Op)

	if logEntry.CommandIndex != logIndex || cmd.Cmd != AppendKey || cmd.Key != args.Key {
		DebugNode(kv.debugStartTime, dInconsistentLogEntry, kv.me, kv.KvNodeState(), "Inconsistent Log Entry at Index = %d, Command = %+v", logIndex, logEntry)
		panic(dInconsistentLogEntry)
	}

	val, ok := kv.kvStore.Load(args.Key)
	newVal := args.Value
	if ok {
		newVal = val.(string) + newVal
	}

	kv.kvStore.Store(args.Key, newVal)

	reply.Err = OK
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.kvStore = &sync.Map{}
	kv.leaderExists.Store(false)
	kv.debugStartTime = time.Now()

	ch := make(chan raft.ApplyMsg)
	kv.applyCh = &ch
	kv.rf = raft.Make(servers, me, persister, ch)
	DebugNode(kv.debugStartTime, dInitNode, kv.me, kv.KvNodeState(), "Created KV node")

	return kv
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
