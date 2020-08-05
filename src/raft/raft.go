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
type LogEntry string

// Raftee is the data associated with an instance of the Raft Algorithm
type Raftee struct {
	mu               sync.Mutex
	l                net.Listener
	companions       []string
	me               int // this Raftee's candidateId
	leadershipStatus leadership
	currentTerm      int
	votedFor         int
	log              []LogEntry
	electionTimeout  time.Duration
	electionTimer    *time.Timer
	dead             int32 // for testing
	rpcCount         int32
	testingID        int64
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

// used for testing communication between raft instances
func (raft *Raftee) appendToOtherLog(address string, toAppend LogEntry) error {
	args := &AppendEntriesArgs{}
	args.Entries = []LogEntry{toAppend}
	reply := &AppendEntriesReply{}
	common.Call(address, "Raftee.AppendEntries", args, reply)
	return nil
}

// becomeCandidate changes the raftee to a candidate and sends out votes.
func (raft *Raftee) becomeCandidate() {
	DPrintfRaft2("Begin becomeCandidate for Raftee(%v)\n", raft.me)
	raft.mu.Lock()
	if raft.leadershipStatus == candidate {
		panic(fmt.Sprintf("Something's gone wrong, Raftee(%v) called becomeCandidate() when already candidate.", raft.me))
	}
	raft.leadershipStatus = candidate
	// TODO: Send out votes
	raft.mu.Unlock()
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
