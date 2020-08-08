package raft

import (
	"common"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// Plan of attack
// Get to the point where I can bring up
// all of the individual Raftees, get them
// to communicate with one another, and kill
// them individually.

// After this works, implement elections.

// DebugRaft ...
const DebugRaft = 0

// DPrintfRaft ...
func DPrintfRaft(format string, a ...interface{}) (n int, err error) {
	if DebugRaft > 0 {
		log.Printf(format, a...)
	}
	return
}

// DebugRaft2 ...
const DebugRaft2 = 0

// DPrintfRaft2 ...
func DPrintfRaft2(format string, a ...interface{}) (n int, err error) {
	if DebugRaft2 > 0 {
		log.Printf(format, a...)
	}
	return
}

type leadership int

const (
	follower leadership = iota + 1
	candidate
	leader
)

const electionTimeoutShortest = 150
const electionTimeoutInterval = 150

// LogEntry is the type that is used for the Raft Log
type LogEntry struct {
	Command string
	Term    int
}

func (l LogEntry) String() string {
	return fmt.Sprintf("{%v, %v}", l.Command, l.Term)
}

// Raftee is the data associated with an instance of the Raft Algorithm
type Raftee struct {
	mu               sync.Mutex
	l                net.Listener
	companions       []string
	me               int // this Raftee's candidateId
	electionTimeout  time.Duration
	electionTimer    *time.Timer
	leadershipStatus leadership
	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry
	// Volatile state
	commitIndex int   // index of highest log entry known to be committed
	lastApplied int   // index of highest log entry applied to state machine
	nextIndex   []int // for each server, index of the next log entry to send to that server
	matchIndex  []int // for each server, index of the highest log entry known to be replicated
	// Testing info
	dead      int32
	rpcCount  int32
	testingID int64
}

// NewRaftee initializes a Raftee object
func NewRaftee(me int, companions []string) *Raftee {
	raft := &Raftee{}
	raft.testingID = common.Nrand()
	raft.companions = companions
	raft.me = me
	raft.leadershipStatus = follower
	raft.electionTimeout = time.Millisecond * time.Duration(rand.Int63n(electionTimeoutInterval)+electionTimeoutShortest)
	raft.electionTimer = time.AfterFunc(raft.electionTimeout, raft.becomeCandidate)
	return raft
}

// Kill shuts down the raftee in order to test a server going down
func (raft *Raftee) Kill() {
	atomic.StoreInt32(&raft.dead, 1)
	if raft.l != nil {
		raft.l.Close()
	}
}

// isdead returns true if the raftee has been killed
func (raft *Raftee) isdead() bool {
	return atomic.LoadInt32(&raft.dead) != 0
}

// Saves the state of the raftee to disk or something like that.
func (raft *Raftee) savePersistentState() {
	// TODO: Implement
}

// Mimic a crash by wiping all volatile state on this machine
func (raft *Raftee) deleteVolatileState() {
	// TODO: Implement
}

// becomeCandidate changes the raftee to a candidate and sends out votes.
func (raft *Raftee) becomeCandidate() {
	DPrintfRaft2("Begin becomeCandidate for Raftee(%v)\n", raft.me)
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if raft.leadershipStatus == candidate {
		panic(fmt.Sprintf("Something's gone wrong, Raftee(%v) called becomeCandidate() when already candidate.", raft.me))
	}
	raft.leadershipStatus = candidate
	// TODO: Send out votes
	DPrintfRaft2("End becomeCandidate for Raftee(%v)\n", raft.me)
}

// Make returns a raftee to an application that wants to use
// the raft algorithm to come to a consensus
// companions is a slice of the addresses of the other raftees
// me is the index that this raftee owns in the companions slice
func Make(me int, companions []string) *Raftee {
	raft := NewRaftee(me, companions)

	rpcs := rpc.NewServer()
	rpcs.Register(raft)
	l, e := net.Listen("tcp", companions[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	raft.l = l

	// Create goroutine to deal with connections
	go func() {
		for raft.isdead() == false {
			conn, err := raft.l.Accept()
			if err == nil && raft.isdead() == false {
				atomic.AddInt32(&raft.rpcCount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			} else if raft.isdead() == false {
				fmt.Printf("Raftee(%v) accept: %v\n", me, err.Error())
			}
		}
	}()

	return raft
}
