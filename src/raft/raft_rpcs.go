package raft

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

	raft.log = append(raft.log, args.Entries...)
	reply.Term = raft.currentTerm
	return nil
}
