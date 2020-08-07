package raft

import (
	"common"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func logSlicesEqual(a, b []LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

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
// func TestLeadership2(t *testing.T) {
// 	rand.Seed(time.Now().Unix())
// 	const nraft = 3
// 	var raftees []*Raftee = make([]*Raftee, nraft)
// 	var addresses []string = make([]string, nraft)
// 	defer cleanup(raftees)
// 	setAddresses(addresses, 5000)
// 	starttime := time.Now()
// 	for i := 0; i < nraft; i++ {
// 		raftees[i] = Make(i, addresses)
// 	}

// 	for i, raftee := range raftees {
// 		if l := raftee.leadershipStatus; l != follower {
// 			t.Fatalf("Expected raftee %v to be follower, was actually %v, time elapsed: %v", i, l, time.Since(starttime))
// 		}
// 	}

// 	time.Sleep(time.Millisecond * time.Duration(electionTimeoutInterval+electionTimeoutShortest))

// 	noLeader := true
// 	for _, raftee := range raftees {
// 		if l := raftee.leadershipStatus; l == leader {
// 			noLeader = false
// 		}
// 	}

// 	if noLeader {
// 		t.Fatalf("Expected some raftee to be a leader, none are.")
// 	}
// }

// Tests that AppendEntries RPC works when we expect it to
func TestAppendEntries0(t *testing.T) {
	const nraft = 2
	var raftees []*Raftee = make([]*Raftee, nraft)
	var addresses []string = make([]string, nraft)
	defer cleanup(raftees)
	setAddresses(addresses, 5000)
	for i := 0; i < nraft; i++ {
		raftees[i] = Make(i, addresses)
	}

	entries := []LogEntry{LogEntry{"My", 1}}
	args := &AppendEntriesArgs{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	}
	// Test appending just one entry to empty log
	reply := &AppendEntriesReply{}
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Failed to append entry, expected %v, got %v\n", entries, raftees[0].log)
	}
	if !reply.Success {
		t.Fatalf("Expected reply to be success, current raft log: %v\n", raftees[0].log)
	}

	// Test appending entry to non-empty log
	entries = append(entries, LogEntry{"name's", 1})
	args.Entries = []LogEntry{entries[1]}
	args.PrevLogIndex = 0
	args.PrevLogTerm = 1
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Failed to append entry, expected %v, got %v\n", entries, raftees[0].log)
	}
	if !reply.Success {
		t.Fatalf("Expected reply to be success, current raft log: %v\n", raftees[0].log)
	}

	// Test appending entries some of which exist in the log
	entries = append(entries, LogEntry{"J", 1})
	args.Entries = entries
	args.PrevLogIndex = -1
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Failed to append entry, expected %v, got %v\n", entries, raftees[0].log)
	}
	if !reply.Success {
		t.Fatalf("Expected reply to be success, current raft log: %v\n", raftees[0].log)
	}

	// Test appending entries that are all already in the log
	args.Entries = entries[1:]
	args.PrevLogIndex = 0
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Failed to append entry, expected %v, got %v\n", entries, raftees[0].log)
	}
	if !reply.Success {
		t.Fatalf("Expected reply to be success, current raft log: %v\n", raftees[0].log)
	}

	// Test overwriting some entries
	args.Entries = []LogEntry{LogEntry{"game's", 2}}
	args.PrevLogIndex = 0
	args.PrevLogTerm = 1
	entries = append(entries[:1], LogEntry{"game's", 2})
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Failed to append entry, expected %v, got %v\n", entries, raftees[0].log)
	}
	if !reply.Success {
		t.Fatalf("Expected reply to be success, current raft log: %v\n", raftees[0].log)
	}
}

// Tests that the AppendEntries RPC fails when expected
func TestAppendEntries1(t *testing.T) {
	const nraft = 2
	var raftees []*Raftee = make([]*Raftee, nraft)
	var addresses []string = make([]string, nraft)
	defer cleanup(raftees)
	setAddresses(addresses, 5000)
	for i := 0; i < nraft; i++ {
		raftees[i] = Make(i, addresses)
	}

	raftees[0].currentTerm = 2
	entries := []LogEntry{LogEntry{"My", 2}, LogEntry{"Name's", 2}, LogEntry{"J", 2}}
	args := &AppendEntriesArgs{
		Term:         2,
		LeaderID:     1,
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Failed to append entry, expected %v, got %v\n", entries, raftees[0].log)
	}
	if !reply.Success {
		t.Fatalf("Expected reply to be success, current raft log: %v\n", raftees[0].log)
	}

	// Test term < currentTerm
	args.Term = 1
	raftees[0].AppendEntries(args, reply)
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Logs don't match, expected %v, got %v\n", entries, raftees[0].log)
	}
	if reply.Success {
		t.Fatalf("Expected reply to be failure, current raft log: %v, reply %v\n", raftees[0].log, reply)
	}
	args.Term = 2
	// test unmatching prevlogterm
	args.Entries = []LogEntry{LogEntry{"Millz", 2}}
	args.PrevLogIndex = 2
	args.PrevLogTerm = 1
	if len(raftees[0].log) != len(entries) || !logSlicesEqual(entries, raftees[0].log) {
		t.Fatalf("Logs don't match, expected %v, got %v\n", entries, raftees[0].log)
	}
	if reply.Success {
		t.Fatalf("Expected reply to be failure, current raft log: %v, reply %v\n", raftees[0].log, reply)
	}

}
