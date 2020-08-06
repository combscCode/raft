package raft

import (
	"common"
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

// Test that raftees start as followers and begin the candidate election phase
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

// Tests that heartbeats prevent raftees from entering the candidate phase.
func TestLeadership1(t *testing.T) {
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

	// Send out heartbeats to each raftee at a rate of 4 times per shortest election timeout possible
	for time.Since(starttime) < 2*time.Millisecond*time.Duration(electionTimeoutInterval+electionTimeoutShortest) {
		time.Sleep(time.Millisecond * time.Duration(electionTimeoutShortest) / 4)
		for _, address := range addresses {
			args := &AppendEntriesArgs{}
			reply := &AppendEntriesReply{}
			common.Call(address, "Raftee.AppendEntries", args, reply)
		}
	}

	for i, raftee := range raftees {
		if l := raftee.leadershipStatus; l != follower {
			t.Fatalf("Expected raftee %v to be follower, was actually %v, time elapsed: %v", i, l, time.Since(starttime))
		}
	}

}

// Tests that someone will become a leader after the group is initialized as all followers.
func TestLeadership2(t *testing.T) {
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

	noLeader := true
	for _, raftee := range raftees {
		if l := raftee.leadershipStatus; l == leader {
			noLeader = false
		}
	}

	if noLeader {
		t.Fatalf("Expected some raftee to be a leader, none are.")
	}
}

// Tests that raftees can communicate with one another and append to each other's logs
func TestAppendEntries0(t *testing.T) {
	const nraft = 2
	var raftees []*Raftee = make([]*Raftee, nraft)
	var addresses []string = make([]string, nraft)
	defer cleanup(raftees)
	setAddresses(addresses, 5000)
	for i := 0; i < nraft; i++ {
		raftees[i] = Make(i, addresses)
	}

	var msg LogEntry = LogEntry{"Potato", 1}
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      []LogEntry{msg},
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != 1 || raftees[0].log[0].Command != msg.Command {
		t.Fatalf("Failed to append entry, expected [[Potato, 1]], got %v", raftees[0].log)
	}

}
