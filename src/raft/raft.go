package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

const ElectionTimeout = 500 * time.Millisecond
const HeartBeatInterval = 150 * time.Millisecond

type RoleType int

const (
	Follower RoleType = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int64
	votedFor    int64
	log         []interface{}
	role        RoleType
	commitIndex int64
	lastApplied int64

	nextIndex  []int64
	matchIndex []int64

	heartbeat chan struct{}
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

//
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
//
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []ApplyMsg
	LeaderCommit int64
}

type AppendEntriesReply struct {
	term    int64
	success bool
}

func (rf *Raft) AppendEntries() {

}

func (rf *Raft) rHeartbeat(lterm chan int64) {
	for idx, peer := range rf.peers {
		if idx != rf.me {
			go func(rf *Raft, peer *labrpc.ClientEnd) {
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				reply := AppendEntriesReply{}
				if ok := peer.Call("Raft.AppendEntries", &args, &reply); ok {
					if !reply.success {
						if reply.term > rf.currentTerm {
							lterm <- reply.term
						}
					}
				} else {
					fmt.Printf("hb no ok, peer: %v", peer)
				}
			}(rf, peer)
		}
	}
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	rf.role = Leader
	rf.mu.Unlock()

	lterm := make(chan int64)
	lticker := time.NewTicker(HeartBeatInterval)
	defer lticker.Stop()

	for {
		select {
		case <- lticker.C: // send heartbeat
			rf.rHeartbeat(lterm)
		case term := <-lterm:
			go rf.convertToFollower(term)
			return
		}
	}
}

func (rf *Raft) rBroadcast(voteChan chan struct{}, cterm chan int64) {
	for idx, peer := range rf.peers {
		if idx != rf.me {
			go func(rf *Raft, peer *labrpc.ClientEnd) {
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				reply := RequestVoteReply{}
				if ok := peer.Call("Raft.RequestVote", &args, &reply); ok {
					if reply.VoteGranted {
						voteChan <- struct{}{}
					} else if reply.Term > rf.currentTerm {
						cterm <- reply.Term
					}
				} else {
					fmt.Printf("RequestVote err, peer: %v", peer)
				}
			}(rf, peer)
		}
	}
}

func randElectionTimeout() time.Duration {
	rand.Seed(time.Now().Unix())
	return (time.Duration)(rand.Intn(150)+350) * time.Microsecond
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	rf.role = Candidate
	rf.currentTerm += 1
	rf.mu.Unlock()

	// broadcast
	ctimer := time.NewTimer(randElectionTimeout())
	defer ctimer.Stop()

	cterm := make(chan int64)
	voteChan := make(chan struct{})
	var voteNum int
	rf.rBroadcast(voteChan, cterm)
	for {
		select {
		case <-ctimer.C:
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.mu.Unlock()
			rf.rBroadcast(voteChan, cterm)
			ctimer.Reset(randElectionTimeout())
		case <-voteChan:
			voteNum += 1
			if voteNum > len(rf.peers)/2 {
				go rf.convertToLeader()
				return
			}
		case term := <-cterm:
			go rf.convertToFollower(term)
			return
		}
	}
}

func (rf *Raft) convertToFollower(term int64) {
	rf.mu.Lock()
	rf.role = Follower
	rf.currentTerm = term
	rf.mu.Unlock()
	electTimer := time.NewTimer(ElectionTimeout)
	defer electTimer.Stop()
	for {
		select {
		case <-rf.heartbeat:
			electTimer.Reset(ElectionTimeout)
		case <-electTimer.C:
			go rf.convertToCandidate()
			return
		}
	}
}

//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartbeat = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.convertToFollower(rf.currentTerm)

	return rf
}
