# MIT 6.5840 Distributed Systems Labs

A series of labs accompanying the coursework on distributed systems.
This repo holds my solutions to these labs.

## Labs

### 1. MapReduce - Not completed

### 2. Linearizable Key/Value Server

This lab involves building a key/value server for a single machine that ensures that each operation is executed exactly once despite network failures and that the operations are linearizable.

The solution passes all the tests.

### 3. Raft

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
