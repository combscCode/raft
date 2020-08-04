package raft

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func cleanup(rafts []*Raftee) {
	for _, raft := range rafts {
		if raft != nil {
			raft.Kill()
		}
	}
}

func setAddresses(addresses []string, startingPort int) {
	for i := 0; i < len(addresses); i++ {
		addresses[i] = "localhost:" + strconv.Itoa(startingPort+i)
	}
}

func TestLeadership0(t *testing.T) {
	rand.Seed(time.Now().Unix())
	const nraft = 3
	var raftees []*Raftee = make([]*Raftee, nraft)
	var addresses []string = make([]string, nraft)
	defer cleanup(raftees)
	setAddresses(addresses, 5000)
	starttime := time.Now()
	for i := 0; i < nraft; i++ {
		raftees[i] = Make(i, addresses)
	}

	for i, raftee := range raftees {
		if l := raftee.leadershipStatus; l != follower {
			t.Fatalf("Expected raftee %v to be follower, was actually %v, time elapsed: %v", i, l, time.Since(starttime))
		}
	}

	time.Sleep(time.Millisecond * time.Duration(electionTimeoutInterval+electionTimeoutShortest))

	allAreFollowers := true
	for _, raftee := range raftees {
		if l := raftee.leadershipStatus; l != follower {
			allAreFollowers = false
		}
	}

	if allAreFollowers {
		t.Fatalf("Expected some raftee to not be a follower, all are.")
	}
}
func TestCommunication0(t *testing.T) {
	const nraft = 2
	var raftees []*Raftee = make([]*Raftee, nraft)
	var addresses []string = make([]string, nraft)
	defer cleanup(raftees)
	setAddresses(addresses, 5000)
	for i := 0; i < nraft; i++ {
		raftees[i] = Make(i, addresses)
	}

	var msg LogEntry = "Potato"
	e := raftees[0].appendToOtherLog(addresses[1], msg)
	if e != nil {
		t.Fatalf("Couldn't append to other log, %v", e)
	}
	e = raftees[0].appendToOtherLog(addresses[1], msg)
	if e != nil {
		t.Fatalf("Couldn't append to other log, %v", e)
	}
	e = raftees[1].appendToOtherLog(addresses[0], msg)
	if e != nil {
		t.Fatalf("Couldn't append to other log, %v", e)
	}
	if len(raftees[1].log) != 2 || raftees[1].log[0] != msg || raftees[1].log[1] != msg {
		t.Fatalf("Append to log didn't go through. Expected [potato], got %v", raftees[1].log)
	}
	if len(raftees[0].log) != 1 || raftees[0].log[0] != msg {
		t.Fatalf("Append to log changed the local log. Expected [], got %v", raftees[0].log)
	}
}
