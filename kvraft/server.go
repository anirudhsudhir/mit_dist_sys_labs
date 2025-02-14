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
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// me
	kvStore            *sync.Map
	assumeLeaderExists *atomic.Bool
	notifyLogEntryBool *atomic.Bool
	notifyLogEntryChan *chan struct{}

	debugStartTime time.Time
}

type stateMachineUpdateArgs struct {
	applyCh            chan raft.ApplyMsg
	kvStore            *sync.Map
	notifyLogEntryBool *atomic.Bool
	notifyLogEntryChan *chan struct{}
	assumeLeaderExists *atomic.Bool

	debugStartTime time.Time
	me             int
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
	kv.assumeLeaderExists = &atomic.Bool{}
	kv.assumeLeaderExists.Store(false)
	kv.notifyLogEntryBool = &atomic.Bool{}
	kv.notifyLogEntryBool.Store(false)
	ch := make(chan struct{})
	kv.notifyLogEntryChan = &ch
	kv.debugStartTime = time.Now()

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	args := &stateMachineUpdateArgs{
		applyCh:            kv.applyCh,
		kvStore:            kv.kvStore,
		assumeLeaderExists: kv.assumeLeaderExists,
		notifyLogEntryBool: kv.notifyLogEntryBool,
		notifyLogEntryChan: kv.notifyLogEntryChan,
		debugStartTime:     time.Now(),
		me:                 kv.me,
	}
	go applyStateMachineUpdates(args)

	DebugNode(kv.debugStartTime, dInitNode, kv.me, kv.KvNodeState(), "Created KV node")

	return kv
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

	if !kv.assumeLeaderExists.Load() {
		time.Sleep(time.Second)
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Submitting Operation to Raft = %+v", op)

	kv.notifyLogEntryBool.Store(true)
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		kv.notifyLogEntryBool.CompareAndSwap(true, false)
		DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Node is not leader")
		reply.Value = ""
		reply.Err = ErrWrongLeader
		return
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Waiting for state machine update by Raft")
	<-*kv.notifyLogEntryChan

	val, ok := kv.kvStore.Load(args.Key)
	if !ok {
		reply.Value = ""
		reply.Err = ErrNoKey
		DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Replying to RPC with response = %+v", reply)
		return
	}

	reply.Value = val.(string)
	reply.Err = OK

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Replying to RPC with response = %+v", reply)
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

	if !kv.assumeLeaderExists.Load() {
		time.Sleep(time.Second)
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Submitting Operation to Raft = %+v", op)

	kv.notifyLogEntryBool.Store(true)
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		kv.notifyLogEntryBool.CompareAndSwap(true, false)
		DebugNode(kv.debugStartTime, dPutKeyNode, kv.me, kv.KvNodeState(), "Node is not leader")
		reply.Err = ErrWrongLeader
		return
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Waiting for state machine update by Raft")
	<-*kv.notifyLogEntryChan

	reply.Err = OK

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Replying to RPC with response = %+v", reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Received a Get Key RPC")

	op := Op{
		AppendKey,
		args.Key,
		args.Value,
	}

	if !kv.assumeLeaderExists.Load() {
		time.Sleep(time.Second)
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Submitting Operation to Raft = %+v", op)

	kv.notifyLogEntryBool.Store(true)
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		kv.notifyLogEntryBool.CompareAndSwap(true, false)
		DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Node is not leader")
		reply.Err = ErrWrongLeader
		return
	}

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Waiting for state machine update by Raft")
	<-*kv.notifyLogEntryChan

	reply.Err = OK

	DebugNode(kv.debugStartTime, dGetKeyNode, kv.me, kv.KvNodeState(), "Replying to RPC with response = %+v", reply)
}

func applyStateMachineUpdates(args *stateMachineUpdateArgs) {
	for {
		logEntry := <-args.applyCh
		args.assumeLeaderExists.CompareAndSwap(false, true)
		DebugNode(args.debugStartTime, dApplyStateMachineUpdates, args.me, "Not filled", "Received log entry from raft = %+v", logEntry)

		cmd := logEntry.Command.(Op)

		switch cmd.Cmd {
		case PutKey:
			args.kvStore.Store(cmd.Key, cmd.Value)
			DebugNode(args.debugStartTime, dApplyStateMachineUpdates, args.me, "Not filled", "Put Operation applied to State Machine")
		case AppendKey:
			val, ok := args.kvStore.Load(cmd.Key)
			newVal := cmd.Value
			if ok {
				newVal = val.(string) + newVal
			}
			args.kvStore.Store(cmd.Key, newVal)
			DebugNode(args.debugStartTime, dApplyStateMachineUpdates, args.me, "Not filled", "Append Operation applied to State Machine")
		case DeleteKey:
			args.kvStore.Delete(cmd.Key)
			DebugNode(args.debugStartTime, dApplyStateMachineUpdates, args.me, "Not filled", "Delete Operation applied to State Machine")
		case GetKey:
			// Nothing to be applied to state machine
		}

		notifyLogEntry := args.notifyLogEntryBool.Load()
		DebugNode(args.debugStartTime, dApplyStateMachineUpdates, args.me, "Not filled", "Value of notifyLogEntry = %t", notifyLogEntry)
		if notifyLogEntry {
			args.notifyLogEntryBool.Store(false)
			DebugNode(args.debugStartTime, dApplyStateMachineUpdates, args.me, "Not filled", "Sending a notification on notifyLogEntryChan")
			*args.notifyLogEntryChan <- struct{}{}
		}
	}
}

func (kv *KVServer) KvNodeState() string {
	_, isLeader := kv.rf.GetState()
	if isLeader {
		return "Leader"
	} else {
		return "NotLeader"
	}
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
