# MIT 6.5840 Distributed Systems Labs

A series of labs accompanying the [coursework on distributed systems](https://pdos.csail.mit.edu/6.824/index.html), including a Raft implementation.
This repo holds my solutions to these labs.

## Labs

### 1. MapReduce - Not completed

### 2. Linearizable Key/Value Server

This lab involves building a key/value server for a single machine that ensures that each operation is executed exactly once despite network failures and that the operations are linearizable.

The solution is present in `kvsrv/{client.go,server.go,common.go}` and passes all tests.

### 3. Raft

This lab involves implementing Raft, a replicated state machine protocol.

The solution is present in `raft/raft.go`

#### 3A: Leader election

Implement Raft leader election and heartbeats.
The goal for Part 3A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost.

The solution passes all tests:

- ✅ Initial election
- ✅ Election after network failure
- ✅ Multiple elections

#### 3B: Log Replication (WIP)

Implement the leader and follower code to append new log entries

The solution passes the following tests:

- ✅ basic agreement
- ✅ RPC byte count
- ✅ Test progressive failure of followers
- ✅ Test failure of leaders
- ✅ Agreement after follower reconnects
- ✅ No agreement if too many followers disconnect
- ❌ Concurrent Start()s
- ❌ Rejoin of partitioned leader
- ❌ Leader backs up quickly over incorrect follower logs
- ⏳ RPC counts aren't too high

## Note

- `labrpc` and `labgob` are packages provided by MIT for performing RPCs. The tester can tell `labrpc` to delay RPCs, re-order them, and discard them to simulate various network failures.
- `porcupine` and `models` are packages used for testing labs

---

## Solutions

### Lab 2

#### Deduplication of requests

- Attach a logical clock and a unique identifier to each Clerk
- Store a map of the Clerk Id to the logical clock on the server
- On every state update, such as a `Put` or `Append` request, increment the logical clock on the client and send a request.
- If the request received contains the timestamp following the one stored on the server, perform the update and increment the clock on the server
- Else, classify the request as duplicate and handle accordingly.
- `Get` requests do not involve state change and hence do not require attachment or incrementing of logical clocks

#### Linearizability during concurrent updates over an unreliable network

- If a duplicate `Append` request is received, use strings.Split to split the value of the given key and return the value expected before this `Append`(As a concurrent update might have appended another value, hence the value expected before this `Append` might be different from the current value on the server)
- This solution was suitable here as `Append` requests involved unique strings. Dealing with non-unique `Append`s might require a better solution

---
