package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anirudhsudhir/mit_dist_sys_labs/labgob"
	"github.com/anirudhsudhir/mit_dist_sys_labs/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	persistentState *PersistentState

	// me: volatile state for all servers
	// commitIndex == lastApplied for leaders
	// for other nodes, commitIndex updated during heartbeats or log replication
	// lastApplied updated after entries are committed
	//
	// me: initialised to 0, when logs are committed, empty log[0] is ignored
	commitIndex int // initialized to 0
	lastApplied int // initialized to 0

	// me: volatile state for leaders
	nextIndex  []int // initialized to last leader log + 1
	matchIndex []int // initialized to 0

	// me: additional variables
	receivedHeartbeatOrVoteGrant bool
	currentRole                  serverCurrentRole
	currentLeader                int            // intialized to -1
	votesReceived                []int          // stores the index of the server in peer[]
	applyChChan                  *chan ApplyMsg // channel to send committed entries

	// me: condition variable to indicate when entries must be applied to the state machine
	applyEntriesCondVar *sync.Cond

	debugStartTime time.Time
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// Custom types and methods

// me: to be persisted
type PersistentState struct {
	CurrentTerm int        // initialized to 0
	VotedFor    int        // intialized to -1
	Log         []LogEntry // indexed from 1
}

// me: This struct stores a particular log entry
type LogEntry struct {
	Command interface{}
	Term    int
}

// me: This struct holds the server state(follower, candidate or leader)
type serverCurrentRole int

// me: defining enumerated values for serverCurrentRole
const (
	Follower serverCurrentRole = iota + 1
	Candidate
	Leader
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func GetCurrentRole(role serverCurrentRole) string {
	switch role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}

	return ""
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.persistentState.CurrentTerm

	if rf.currentRole == Leader {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.persistentState)
	rf.persister.Save(buf.Bytes(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.persistentState = &PersistentState{
			VotedFor: -1,
		}

		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&rf.persistentState); err != nil {
		Debug(rf.debugStartTime, dPersist, rf.me, GetCurrentRole(rf.currentRole), "Failed to decode persistent state from disk -> %v", err)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.persistentState.CurrentTerm {
		Debug(rf.debugStartTime, dRequestVoteHandler, rf.me, GetCurrentRole(rf.currentRole), "Failed to granting vote to request with args = %+v", args)
		reply.VoteGranted = false
		reply.Term = rf.persistentState.CurrentTerm
	} else {
		if args.Term > rf.persistentState.CurrentTerm {
			rf.mu.Unlock()
			rf.transitionToHigherTerm(args.Term)
			rf.mu.Lock()
		}

		lastLogIndex := 0
		lastLogTerm := 0
		if len(rf.persistentState.Log) >= 2 {
			lastLogIndex = len(rf.persistentState.Log) - 1
			lastLogTerm = rf.persistentState.Log[lastLogIndex].Term
		}

		// me: election safety restriction
		// log of the candidate requesting vote must be up to date or greater than the current follower's log
		logOk := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
		Debug(rf.debugStartTime, dRequestVoteHandler, rf.me, GetCurrentRole(rf.currentRole), "Checking vote request with args = %+v, current node lastLogTerm = %d, lastLogIndex= %d, logOk:= %v", args, lastLogTerm, lastLogIndex, logOk)
		if args.Term == rf.persistentState.CurrentTerm && logOk && (rf.persistentState.VotedFor == -1 || rf.persistentState.VotedFor == args.CandidateId) {
			Debug(rf.debugStartTime, dRequestVoteHandler, rf.me, GetCurrentRole(rf.currentRole), "Granting vote to request with args = %+v", args)
			reply.VoteGranted = true
			reply.Term = rf.persistentState.CurrentTerm
			rf.persistentState.VotedFor = args.CandidateId
			rf.receivedHeartbeatOrVoteGrant = true

			rf.persist()
		} else {
			Debug(rf.debugStartTime, dRequestVoteHandler, rf.me, GetCurrentRole(rf.currentRole), "Failed to grant vote to request with args = %+v", args)
			reply.VoteGranted = false
			reply.Term = rf.persistentState.CurrentTerm
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()

	if rf.currentRole == Leader {
		Debug(rf.debugStartTime, dStart, rf.me, GetCurrentRole(rf.currentRole), "Received a new Entry to start consensus = %+v", command)
		logEntry := LogEntry{
			Term:    rf.persistentState.CurrentTerm,
			Command: command,
		}

		// me: appending a dummy LogEntry at index 0
		// indexing in the log starts from 1 (according to the paper)
		if len(rf.persistentState.Log) == 0 {
			rf.persistentState.Log = append(rf.persistentState.Log, LogEntry{})
		}

		rf.persistentState.Log = append(rf.persistentState.Log, logEntry)

		rf.persist()

		rf.nextIndex[rf.me] = len(rf.persistentState.Log)
		rf.matchIndex[rf.me] = len(rf.persistentState.Log) - 1

		go rf.performLogBroadcast()

		index = rf.matchIndex[rf.me]
		term = rf.persistentState.CurrentTerm
		isLeader = true
		Debug(rf.debugStartTime, dStart, rf.me, GetCurrentRole(rf.currentRole), "Starting agreement on new entry, cmd = %+v", command)
	}

	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// me: handles the entirety of leader election

		// pause for a random amount of time between 200 and 400 ms
		// me: Wait for heartbeat or RequestVope RPCs
		ms := 400 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// me: if current server is the leader, prevent election start
		rf.mu.Lock()
		serverRole := rf.currentRole
		rf.mu.Unlock()

		for serverRole == Leader && !rf.killed() {
			// me: Sleep for 200 to 400 ms before checking if current node is leader
			// me: if current node is leader, repeat process
			ms := 400 + (rand.Int63() % 200)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			rf.mu.Lock()
			serverRole = rf.currentRole
			rf.mu.Unlock()
		}

		var args *RequestVoteArgs
		continueElection := false

		rf.mu.Lock()

		if !rf.receivedHeartbeatOrVoteGrant {
			rf.currentRole = Candidate
			rf.persistentState.CurrentTerm += 1
			rf.persistentState.VotedFor = rf.me
			rf.votesReceived = rf.votesReceived[:0]
			rf.votesReceived = append(rf.votesReceived, rf.me)
			continueElection = true

			rf.persist()

			args = &RequestVoteArgs{
				Term:        rf.persistentState.CurrentTerm,
				CandidateId: rf.me,
			}

			if len(rf.persistentState.Log) >= 2 {
				args.LastLogIndex = len(rf.persistentState.Log) - 1
			}

			// me: as a leader election always increments term,
			// term 0 implies no log entry
			// if log exists, term is set to log term
			if args.LastLogIndex > 0 {
				args.LastLogTerm = rf.persistentState.Log[args.LastLogIndex].Term
			}

		} else {
			// me: waiting for the next heartbeat
			rf.receivedHeartbeatOrVoteGrant = false
		}
		rf.mu.Unlock()

		if continueElection {
			for targetPeerIndex := range len(rf.peers) {
				// me: rf.me does not change, hence no locks required
				if targetPeerIndex != rf.me {
					go rf.PerformVoteRequest(targetPeerIndex, args)
				}
			}

			// me: Election timeout - Waits for a round of election for 100 to 200ms
			electionTimer := 200 + (rand.Int63() % 200)
			time.Sleep(time.Duration(electionTimer) * time.Millisecond)

			rf.mu.Lock()

			if rf.currentRole == Candidate {
				// me: start new election
				rf.mu.Unlock()
				continue
			}

			rf.mu.Unlock()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	rf := &Raft{}
	rf.debugStartTime = time.Now()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentRole = Follower
	rf.currentLeader = -1
	rf.applyChChan = &applyCh

	rf.applyEntriesCondVar = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// me: applying entries to the state machine in a separate goroutine
	go rf.applyLogEntries()

	Debug(rf.debugStartTime, dMake, rf.me, GetCurrentRole(rf.currentRole), "Created new raft instance")
	return rf
}

// me: sends a vote request to a particular node and processes the response
func (rf *Raft) PerformVoteRequest(targetPeerIndex int, args *RequestVoteArgs) {
	var reply RequestVoteReply
	receivedReply := rf.sendRequestVote(targetPeerIndex, args, &reply)

	rf.mu.Lock()

	if rf.currentRole == Follower || rf.currentRole == Leader || !receivedReply {
		rf.mu.Unlock()
		return
	}

	if reply.Term == rf.persistentState.CurrentTerm && reply.VoteGranted {
		if !slices.Contains(rf.votesReceived, targetPeerIndex) {
			rf.votesReceived = append(rf.votesReceived, targetPeerIndex)
		}

		// me: ensuring votes received is greater than quorum
		if len(rf.votesReceived) >= int(math.Ceil(float64(len(rf.peers)+1)/2)) {
			rf.currentRole = Leader
			rf.currentLeader = rf.me
			Debug(rf.debugStartTime, dVote, rf.me, GetCurrentRole(rf.currentRole), "NODE HAS BECOME LEADER!")

			rf.nextIndex = rf.nextIndex[:0]
			rf.matchIndex = rf.matchIndex[:0]

			nextLogIndex := -1
			if len(rf.persistentState.Log) == 0 {
				nextLogIndex = 1
			} else {
				// me: a log with entries will contain a dummy entry at index 0
				// hence, len(rf.log) of the log is (1 + actual entries)
				// length of the log = last log index = len(rf.log)-1
				// therefore, nextLogIndex = len(rf.log)
				nextLogIndex = len(rf.persistentState.Log)
			}
			for range len(rf.peers) {
				rf.nextIndex = append(rf.nextIndex, nextLogIndex)
				rf.matchIndex = append(rf.matchIndex, 0)
			}

			go rf.sendHeartBeats()
		}

	} else if reply.Term > rf.persistentState.CurrentTerm {
		rf.mu.Unlock()
		rf.transitionToHigherTerm(reply.Term)
		return
	}

	rf.mu.Unlock()
}

// me: sends heartbeats at random intervals during idle
func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {

		for follower := range len(rf.peers) {
			if follower != rf.me {
				// me: sending heartbeats in parallel
				go rf.replicateLog(rf.me, follower)
			}
		}

		// me: Wait between 150 and 250ms to send heartbeats (4 to 5 heartbeats per second)
		ms := 150 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		serverRole := rf.currentRole
		rf.mu.Unlock()

		if serverRole != Leader {
			break
		}
	}
}

// me: performs a log replication request or heartbeat
func (rf *Raft) replicateLog(leaderPeerIndex int, followerPeerIndex int) {
	if rf.killed() {
		return
	}

	// me: preparing RPC request
	var reply AppendEntriesReply
	rf.mu.Lock()

	if rf.currentRole != Leader {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()
	rf.checkAndUpdateCommitIndex()
	rf.mu.Lock()

	args := &AppendEntriesArgs{
		Term:         rf.persistentState.CurrentTerm,
		LeaderId:     leaderPeerIndex,
		LeaderCommit: rf.commitIndex,
	}

	// me: avoiding unnecessary operations for empty logs
	if len(rf.persistentState.Log) > 0 {
		// me: PrevLogLength holds the length of the expected follower log before append
		// Length of the log includes the actual commands, starting from index 1
		// It does not include the dummy log at index 0
		// The length of the log is therefore the same as the last log index

		args.PrevLogIndex = rf.nextIndex[followerPeerIndex] - 1

		for i := rf.nextIndex[followerPeerIndex]; i < len(rf.persistentState.Log); i++ {
			args.Entries = append(args.Entries, rf.persistentState.Log[i])
		}

		if args.PrevLogIndex >= 1 {
			args.PrevLogTerm = rf.persistentState.Log[args.PrevLogIndex].Term
		}
	}

	Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "Sending AppendEntriesRPC to node %d with PrevLogIndex = %d, PrevLogTerm = %d, logs = %+v", followerPeerIndex, args.PrevLogIndex, args.PrevLogTerm, rf.persistentState.Log)
	rf.mu.Unlock()

	receivedReply := rf.peers[followerPeerIndex].Call("Raft.AppendEntries", args, &reply)

	if receivedReply {
		rf.mu.Lock()
		if rf.persistentState.CurrentTerm == reply.Term {

			// processing response only if it is a valid reply to an RPC sent in the current term
			if rf.persistentState.CurrentTerm == args.Term && rf.currentRole == Leader {

				if reply.Success {

					if len(args.Entries) > 0 {
						rf.matchIndex[followerPeerIndex] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[followerPeerIndex] = args.PrevLogIndex + len(args.Entries) + 1
					}

					Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "Successful AppendEntriesRPC to node %d, nextIndex of nodes = %+v", followerPeerIndex, rf.nextIndex)

					rf.mu.Unlock()
					rf.checkAndUpdateCommitIndex()

					return
				} else if rf.nextIndex[followerPeerIndex] > 0 {
					rf.nextIndex[followerPeerIndex] -= 1

					rf.mu.Unlock()
					rf.replicateLog(leaderPeerIndex, followerPeerIndex)

					return
				}
			} // end of if rf.persistentState.currentTerm == args.Term

		} else if rf.persistentState.CurrentTerm < reply.Term {
			rf.mu.Unlock()
			rf.transitionToHigherTerm(reply.Term)

			return
		}

		rf.mu.Unlock()
	}
}

// me: handles an AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "Received AppendEntriesRPC from node = %d, with prevLogIndex = %d and PrevLogTerm = %d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)

	reply.Term = args.Term
	reply.Success = true
	rf.receivedHeartbeatOrVoteGrant = true

	if args.Term > rf.persistentState.CurrentTerm {
		Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "Rejecting AppendEntries with args.Term = %d, rf.currentTerm = %d", args.Term, rf.persistentState.CurrentTerm)
		rf.mu.Unlock()
		rf.transitionToHigherTerm(args.Term)
		rf.mu.Lock()

		rf.currentLeader = args.LeaderId
	} else if args.Term < rf.persistentState.CurrentTerm {

		// Reply false if term<currentTerm (§5.1)
		Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "Received AppendEntriesRPC with older term from node = %d with term = %d, rejecting with latest term %d", args.LeaderId, args.Term, rf.persistentState.CurrentTerm)
		reply.Term = rf.persistentState.CurrentTerm
		reply.Success = false
		rf.receivedHeartbeatOrVoteGrant = false

		rf.mu.Unlock()
		return

	} else if args.Term == rf.persistentState.CurrentTerm {
		rf.currentRole = Follower
		rf.currentLeader = args.LeaderId
	}

	// if len(args.Entries) > 0 {
	// length of the log = last index of the log
	lastLogIndex := -1
	if len(rf.persistentState.Log) == 0 {
		lastLogIndex = 0
	} else {
		// me: len(rf.log) = sum of actual entries and the dummy log at index 0
		lastLogIndex = len(rf.persistentState.Log) - 1
	}

	// me: check if the log is longer or equal in length to expected length on leader to prevent out of bounds indexing
	//
	// if false, then reply.Success retains default value of false
	// and nextIndex is decremented on leader, process continues until log lengths match
	if lastLogIndex >= args.PrevLogIndex {
		if args.PrevLogIndex > 0 {

			// If an existing entry conflicts with a new one (same index
			// but different terms),delete the existing entry and all that
			// follow it (§5.3)
			if rf.persistentState.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
				Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "tRUNCATING LOG AS TERMS DO NOT MATCH FROM LASTiNDEX = %d TO pREVlOGiNDEX - 1 = %d", lastLogIndex, args.PrevLogIndex-1)
				rf.persistentState.Log = rf.persistentState.Log[:args.PrevLogIndex] //truncating log to 0-args.PrevLogIndex-1
				if len(rf.persistentState.Log) == 1 {
					rf.persistentState.Log = rf.persistentState.Log[:0]
				}

				rf.persist()

				// Reply false if log doesn’t contain an entry at prevLogIndex
				// whose term matches prevLogTerm (§5.3)
				//
				// if false, then reply.Success retains default value of false
				rf.mu.Unlock()

				reply.Success = false
				return
			}
		}

		if len(args.Entries) > 0 {
			if lastLogIndex > args.PrevLogIndex {
				if lastLogIndex == 0 {
					panic("[AppendEntries] LastLogIndex = 0 and greater than args.PrevLogIndex")
				}

				// If an existing entry conflicts with a new one (same index
				// but different terms),delete the existing entry and all that
				// follow it (§5.3)

				checkFromArrIndex := args.PrevLogIndex + 1
				for _, entry := range args.Entries {
					if checkFromArrIndex > lastLogIndex {
						break
					}

					if rf.persistentState.Log[checkFromArrIndex].Term != entry.Term {

						oldEntryTerm := rf.persistentState.Log[checkFromArrIndex].Term

						rf.persistentState.Log = rf.persistentState.Log[:checkFromArrIndex]
						lastLogIndex = len(rf.persistentState.Log) - 1

						if len(rf.persistentState.Log) <= 1 {
							rf.persistentState.Log = rf.persistentState.Log[:0]
							lastLogIndex = 0
						}

						Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "DELETING LOG AS OLD TERMS DO NOT MATCH NEW TERMS at log index = %d, current log term = %d, leader entry term = %d, old lastLogIndex = %d ,updatedLog = %+v", checkFromArrIndex, oldEntryTerm, entry.Term, lastLogIndex, rf.persistentState.Log)
						break
					}
				}

				for i := lastLogIndex - args.PrevLogIndex; i < len(args.Entries); i++ {
					if len(rf.persistentState.Log) == 0 {
						rf.persistentState.Log = append(rf.persistentState.Log, LogEntry{})
					}

					rf.persistentState.Log = append(rf.persistentState.Log, args.Entries[i])

					rf.persist()

					Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "Appending entries to log, old index = %d, new index = %d", lastLogIndex, len(rf.persistentState.Log)-1)
				}
			} else {
				// lastLogIndex == args.PrevLogIndex

				if lastLogIndex == 0 {
					rf.persistentState.Log = append(rf.persistentState.Log, LogEntry{})
				}
				rf.persistentState.Log = append(rf.persistentState.Log, args.Entries...)

				rf.persist()

				Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "Appending entries to log, old index = %d, new index = %d", lastLogIndex, len(rf.persistentState.Log)-1)
			}
		}
	} else {
		reply.Success = false
	} // end of if which checks if follower log is shorter

	// If leaderCommit > commitIndex, setcommitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := len(rf.persistentState.Log) - 1
		if lastLogIndex < 0 {
			lastLogIndex = 0
		}
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastLogIndex)))
		Debug(rf.debugStartTime, dApplyLogEntries, rf.me, GetCurrentRole(rf.currentRole), "Follower: Signalling on CondVar to apply log entries, commitIndex = %d, lastApplied = %d", rf.commitIndex, rf.lastApplied)
		rf.applyEntriesCondVar.Signal()
	}

	Debug(rf.debugStartTime, dAppendEntries, rf.me, GetCurrentRole(rf.currentRole), "AppendEntriesRPC handled from node = %d, with prevLogIndex = %d and PrevLogTerm = %d, logs = %+v", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, rf.persistentState.Log)
	rf.mu.Unlock()
}

// me: This function appends an entry to the leader's log and broadcasts it to all nodes
func (rf *Raft) performLogBroadcast() {
	for node := range rf.peers {
		if node != rf.me {
			// me: Sending concurrent log replication requests to all nodes
			// sending in parallel as recommended in the paper and labs
			go rf.replicateLog(rf.me, node)
		}
	}
}

func (rf *Raft) checkAndUpdateCommitIndex() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(rf.debugStartTime, dCommitIndex, rf.me, GetCurrentRole(rf.currentRole), "Starting check to update commit index")

	// me: checking if entry has been applied to quorum
	//
	// last log index of leader
	lastLogIndex := len(rf.persistentState.Log) - 1
	if lastLogIndex > 0 && lastLogIndex > rf.commitIndex {

		quorumNodes := int(math.Ceil(float64(len(rf.peers)+1) / 2))
		N := -1

		// me: looping from index 1 to find highest log index across all nodes
		for i := 1; i <= lastLogIndex; i++ {
			fullyReplicatedNodes := 0
			for _, nodeLatestIndex := range rf.matchIndex {
				if nodeLatestIndex >= i {
					fullyReplicatedNodes++
				}
			}

			if fullyReplicatedNodes >= quorumNodes {
				N = i
			}
		}

		// Debug(rf.debugStartTime, dCommitIndex, rf.me, GetCurrentRole(rf.currentRole), "Results of check to update commit index, N = %d, quorumNodes = %d", N, quorumNodes)

		if N != -1 && N > rf.commitIndex {
			if rf.persistentState.Log[N].Term == rf.persistentState.CurrentTerm {
				Debug(rf.debugStartTime, dApplyLogEntries, rf.me, GetCurrentRole(rf.currentRole), "Leader: Signalling on CondVar to apply log entries, old commitIndex = %d, new commitIndex = %d, lastApplied = %d", rf.commitIndex, N, rf.lastApplied)

				rf.commitIndex = N
				rf.applyEntriesCondVar.Signal()
			}
		}
	}
}

// me: This function commits log entries to the state machine
// Using a condition variable to determine when entries must be applied
func (rf *Raft) applyLogEntries() {

	for !rf.killed() {
		rf.mu.Lock()

		rf.applyEntriesCondVar.Wait()

		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.persistentState.Log[i].Command,
					CommandIndex: i,
				}

				*rf.applyChChan <- applyMsg
				rf.lastApplied = i
				Debug(rf.debugStartTime, dApplyLogEntries, rf.me, GetCurrentRole(rf.currentRole), "Applied log entry, commitIndex = %d, lastApplied = %d, command = %+v, logs = %+v", rf.commitIndex, rf.lastApplied, rf.persistentState.Log[i].Command, rf.persistentState.Log)
			}

		}

		rf.mu.Unlock()

	}
}

func (rf *Raft) transitionToHigherTerm(newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persistentState.CurrentTerm = newTerm
	rf.persistentState.VotedFor = -1

	rf.persist()

	// me: setting length to 0 while retaining allocated memory
	rf.nextIndex = rf.nextIndex[:0]
	rf.matchIndex = rf.matchIndex[:0]

	rf.receivedHeartbeatOrVoteGrant = true
	rf.currentRole = Follower
	rf.currentLeader = -1
	rf.votesReceived = rf.votesReceived[:0]
}
