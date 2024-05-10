package kvsrv

import (
	"log"
	"strings"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]string
	// me: stores a mapping of nodes to respective logical clocks
	requestsLog map[int64]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.store[args.Key]

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.LamportClock-kv.requestsLog[args.Id] == 1 {
		kv.store[args.Key] = args.Value
		kv.requestsLog[args.Id] += 1
	}
	reply.Value = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.LamportClock-kv.requestsLog[args.Id] == 1 {
		val := kv.store[args.Key]
		kv.store[args.Key] = val + args.Value
		kv.requestsLog[args.Id] += 1
		reply.Value = val
	} else {
		reply.Value = strings.Split(kv.store[args.Key], args.Value)[0]
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.requestsLog = make(map[int64]int)
	return kv
}
