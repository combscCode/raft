package raft

// AddToLogArgs is the args required for the corresponding RPC call
type AddToLogArgs struct {
	ToAdd LogEntry
}

// AddToLogReply is the reply required for the corresponding RPC call
type AddToLogReply struct {
	Success bool
}

// AddToLog is used for testing connections between raft instances
func (raft *Raftee) AddToLog(args *AddToLogArgs, reply *AddToLogReply) error {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	raft.log = append(raft.log, args.ToAdd)
	reply.Success = true
	return nil
}
