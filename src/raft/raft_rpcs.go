package raft

import (
	"common"
	"fmt"
)

// AppendEntriesArgs is the args specified in the white paper
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	UniqueID     int64 // Used for testing purposes
}

// AppendEntriesReply is the response specified in the white paper
type AppendEntriesReply struct {
	Term     int
	Success  bool
	UniqueID int64 // Used for testing purposes
}

// AppendEntries is currently only used for heartbeat.
func (raft *Raftee) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	// Deal with Heartbeat logic
	// If Stop() returns true, no need to reset timer because raftee has become radicalized.
	if raft.electionTimer.Stop() {
		raft.electionTimer.Reset(raft.electionTimeout)
	}
	reply.Success = true
	reply.Term = raft.currentTerm

	// request has a stale term number, reject the request
	if args.Term < raft.currentTerm {
		reply.Success = false
		return nil
	}
	// Inconsistent logs between leader and follower
	if len(raft.log) < args.PrevLogIndex+1 || (args.PrevLogIndex > 0 && raft.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return nil
	}
	// If we know the local log is at least as long as the leader's log, check that the entries match
	matchingEntries := 0
	if len(raft.log) > args.PrevLogIndex+1 {
		for i := 0; i < len(args.Entries) && (i+args.PrevLogIndex+1) < len(raft.log); i++ {
			// If an existing entry conflicts with a new one, delete the entry and all that follow
			// checking term and index should be all that's required.
			leaderEntry := args.Entries[i]
			raftEntry := raft.log[i+args.PrevLogIndex+1]
			if leaderEntry.Term != raftEntry.Term {
				raft.log = raft.log[:i+args.PrevLogIndex+1]
				break
			} else {
				if leaderEntry.Command != raftEntry.Command {
					panic(fmt.Sprintf(
						"Log Matching Property violated. Raftee(%v), local log: %v\n"+
							"AppendEntryArgs: UniqueId(%v), LeaderID(%v), Entries(%v)\n",
						raft.me, raft.log, args.UniqueID, args.LeaderID, args.Entries))
				}
				matchingEntries++
			}
		}
	}
	// Append all entries that aren't already in the local log
	raft.log = append(raft.log, args.Entries[matchingEntries:]...)
	if args.LeaderCommit > raft.commitIndex {
		raft.commitIndex = common.Min(args.LeaderCommit, len(raft.log)-1)
	}
	return nil
}
