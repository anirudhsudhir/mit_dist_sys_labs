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

	"log"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

	// me: to be persisted
	currentTerm int        // initialized to 0
	votedFor    int        // intialized to -1
	log         []LogEntry // indexed from 1

	// me: volatile state for all servers
	commitIndex int // initialized to 0
	lastApplied int // initialized to 0

	// me: volatile state for leaders
	nextIndex  []int // initialized to last leader log + 1
	matchIndex []int // initialized to 0

	// me: additional variables
	receivedHeartbeatOrVoteGrant bool
	currentRole                  serverCurrentRole
	currentLeader                int   // intialized to -1
	votesReceived                []int // stores the index of the server in peer[]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm

	// log.Printf("%d checking if it is the leader in term %d", rf.me, rf.currentTerm)
	if rf.currentRole == Leader {
		isleader = true
		// log.Printf("%d knows it is the leader in term %d", rf.me, rf.currentTerm)
	}
	//  else {
	// 	log.Printf("%d knows it is not the leader in term %d", rf.me, rf.currentTerm)
	// }
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// me: logging response
	defer func() {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		// log.Printf("RequestVote RPC, Sender(Wishes to be leader): %d, Receiver: %d, Term: %d, Success: %t, Voted For: %d\n", args.CandidateId, rf.me, reply.Term, reply.VoteGranted, rf.votedFor)
	}()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.currentRole = Follower
			rf.votedFor = -1
		}

		lastLogTerm := 0
		if len(rf.log) >= 2 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}

		argsLogLength := 0
		if args.LastLogIndex >= 1 {
			argsLogLength = args.LastLogIndex + 1
		}

		logOk := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && argsLogLength >= len(rf.log))
		if args.Term == rf.currentTerm && logOk && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.receivedHeartbeatOrVoteGrant = true
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
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
	isLeader := true

	// Your code here (3B).

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

		// pause for a random amount of time between 200 and 500
		// milliseconds.
		// me: Wait for heartbeat or RequestVope RPCs
		ms := 200 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// me: if current server is the leader, prevent election start
		rf.mu.Lock()
		serverRole := rf.currentRole
		rf.mu.Unlock()

		for serverRole == Leader && !rf.killed() {
			ms := 50 + (rand.Int63() % 300)
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
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.votesReceived = rf.votesReceived[:0]
			rf.votesReceived = append(rf.votesReceived, rf.me)
			continueElection = true

			args = &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}

			if len(rf.log) >= 2 {
				args.LastLogIndex = len(rf.log) - 1
			}

			// me: as a leader election always increments term,
			// term 0 implies no log entry
			// if log exists, term is set to log term
			if args.LastLogIndex > 0 {
				args.LastLogTerm = rf.log[args.LastLogIndex].Term
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

			// me: Election timeout - Waits for a round of election
			electionTimer := 50 + (rand.Int63() % 100)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentRole = Follower
	rf.votedFor = -1
	rf.currentLeader = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// Custom types and methods

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
	Term     int
	LeaderId int
	// TODO: FILL MISSING FIELDS
	// Entries []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// me: sends a vote request to a particular node and processes the response
func (rf *Raft) PerformVoteRequest(targetPeerIndex int, args *RequestVoteArgs) {
	var reply RequestVoteReply
	receivedReply := rf.sendRequestVote(targetPeerIndex, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole == Follower || rf.currentRole == Leader || !receivedReply {
		return
	}

	if reply.Term == rf.currentTerm && reply.VoteGranted {
		if !slices.Contains(rf.votesReceived, targetPeerIndex) {
			rf.votesReceived = append(rf.votesReceived, targetPeerIndex)
		}

		// me: ensuring votes received is greater than quorum
		if len(rf.votesReceived) >= int(math.Ceil(float64(len(rf.peers)+1)/2)) {
			// log.Printf("%d HAS BECOME LEADER, votedFor : %+v", rf.me, rf.votesReceived)
			rf.currentRole = Leader
			rf.currentLeader = rf.me
			rf.nextIndex = rf.nextIndex[:0]
			rf.matchIndex = rf.matchIndex[:0]

			for follower := range len(rf.peers) {
				if follower != rf.me {
					rf.nextIndex = append(rf.nextIndex, len(rf.log))
					rf.matchIndex = append(rf.matchIndex, 0)
				}
			}

			go rf.sendHeartBeats()
		}

	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentRole = Follower
		rf.votedFor = -1
		// me: setting length to 0 while retaining allocated memory
		rf.votesReceived = rf.votesReceived[:0]
	}
}

// me: sends heartbeats at random intervals during idle
func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {

		for follower := range len(rf.peers) {
			if follower != rf.me {
				rf.sendAppendEntries(rf.me, follower)
			}
		}

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
// TODO: ADD LOG REPLICATION; ONLY SUPPORTS HEARTBEATS CURRENTLY
func (rf *Raft) sendAppendEntries(leaderPeerIndex int, followerPeerIndex int) {
	var reply AppendEntriesReply
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: leaderPeerIndex,
	}
	rf.mu.Unlock()

	rf.peers[followerPeerIndex].Call("Raft.AppendEntries", args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.currentRole = Follower
		rf.votedFor = -1

		// me: setting length to 0 while retaining allocated memory
		rf.votesReceived = rf.votesReceived[:0]
	}
}

// me: handles an AppendEntries RPC
// TODO: ADD LOG REPLICATION; ONLY SUPPORTS HEARTBEATS CURRENTLY
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.receivedHeartbeatOrVoteGrant = true
		rf.currentLeader = args.LeaderId

		reply.Term = args.Term
		reply.Success = true
	}
}
