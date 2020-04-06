package raft

// RPC interface exposed by each server
// See Figure 2 of paper

// AppendEntriesArgs arguments sent in AppendEntry() RPC
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index log index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// AppendEntriesReply results from AppendEntry() RPC
type AppendEntriesResults struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat.
func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {
	// TODO
}

// AppendEntriesArgs arguments sent in AppendEntry() RPC
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// AppendEntriesResp results from AppendEntry() RPC
type RequestVoteResults struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote is invoked by candidates to gather votes
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteResults) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.currentTerm == args.Term && r.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, r.currentTerm
		return
	}
	if r.currentTerm > args.Term || // valid candidate
		(r.currentTerm == args.Term && r.votedFor != -1) { // the server has voted in this term
		reply.Term, reply.VoteGranted = r.currentTerm, false
		return
	}
	if args.Term > r.currentTerm {
		r.currentTerm, r.votedFor = args.Term, -1
		if r.role != Follower { // once server becomes follower, it has to reset timer
			r.timer = randElectionTimer()
			r.role = Follower
		}
	}
	r.leader = -1 // other server trying to elect a new leader
	reply.Term = args.Term
	lastLogIndex := r.logIndex - 1
	lastLogTerm := r.log[lastLogIndex].Term
	if lastLogTerm > args.LastLogTerm || // the server has log with higher term
		(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) { // under same term, this server has longer index
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	r.votedFor = args.CandidateId
	r.timer = randElectionTimer() // granting vote to candidate, reset timer
	r.saveState()
}
