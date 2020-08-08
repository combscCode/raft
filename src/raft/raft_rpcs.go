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

// RequestVoteArgs ...
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	UniqueID     int64
}

// RequestVoteReply ...
type RequestVoteReply struct {
	Term        int
	VoteGranted Bool
	UniqueID    int64
}

// AppendEntries is used by the leader to coordinate with followers
func (raft *Raftee) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	defer raft.savePersistentState()

	// What to do about race condition here, what happens if we receive an appendentries from a new
	// leader, lock the mutex, the electionTimer gets triggered, and we end up becoming a candidate
	// even though a new leader already exists...

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

// RequestVote is used to collect votes in the candidate election process
func (raft *Raftee) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	defer raft.savePersistentState()

	// Deal with Heartbeat logic
	// If Stop() returns true, no need to reset timer because raftee has become radicalized.
	if raft.electionTimer.Stop() {
		raft.electionTimer.Reset(raft.electionTimeout)
	}

	reply.Term = raft.currentTerm
	// If the RPC has a lower term than the local current term, reject the RPC
	if args.Term < raft.currentTerm {
		reply.VoteGranted = false
		return nil
	}
	// If the RPC has a less up-to-date log than the local log, reject the RPC
	if args.LastLogTerm < raft.currentTerm || (args.LastLogTerm == raft.currentTerm && args.LastLogIndex < len(raft.log)-1) {
		reply.VoteGranted = false
		return nil
	}
	// If we have already voted for someone else, reject the RPC
	if raft.votedFor != -1 && raft.votedFor != args.CandidateID {
		reply.VoteGranted = false
		return nil
	}
	// Vote for the candidate
	raft.votedFor = args.CandidateID
	reply.VoteGranted = true
	return nil
}
