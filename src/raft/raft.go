package raft

import (
	"common"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
)

// Plan of attack
// Get to the point where I can bring up
// all of the individual Raftees, get them
// to communicate with one another, and kill
// them individually.

// After this works, implement elections.

type leadership int

const (
	follower leadership = iota + 1
	candidate
	leader
)

// LogEntry is the type that is used for the Raft Log
type LogEntry string

// Raftee is the data associated with an instance of the Raft Algorithm
type Raftee struct {
	mu               sync.Mutex
	l                net.Listener
	companions       []string
	me               int // this Raftee's candidateId
	LeadershipStatus leadership
	currentTerm      int
	votedFor         int
	log              []LogEntry
	dead             int32 // for testing
	rpcCount         int32
}

// NewRaftee initializes a Raftee object
func NewRaftee(me int, companions []string) *Raftee {
	raft := &Raftee{}
	raft.companions = companions
	raft.me = me
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
	args := &AddToLogArgs{toAppend}
	reply := &AddToLogReply{}
	common.Call(address, "Raftee.AddToLog", args, reply)
	return nil
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
