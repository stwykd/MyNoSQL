package raft

import (
	"log"
	"time"
)

// RPC interface exposed by each Raft server
// See Figure 2 of paper
// RPC calls can take a long while to arrive - when replying, the code may have moved on
// and it's important to gracefully give up in such cases.

// AppendEntriesArgs arguments sent in AppendEntry() RPC
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	//PrevLogIndex int        // index log index of log entry immediately preceding new ones
	//PrevLogTerm  int        // term of prevLogIndex entry
	//Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	//LeaderCommit int        // leader’s commitIndex
}

// AppendEntriesReply results from AppendEntry() RPC
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%v] received AppendEntries: %+v", rf.me, args)
	if rf.state == Down {
		return nil
	}
	if args.Term > rf.currentTerm {
		log.Printf("[%v] currentTerm=%d out of date with AppendEntriesArgs.Term=%d",
			rf.me, rf.currentTerm, args.Term)
		rf.toFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		// two leaders can't coexist. if Raft server receives AppendEntries() RPC, another
		// leader already exists in this term
		if rf.state != Follower {
			rf.toFollower(args.Term)
		}
		rf.resetElection = time.Now()
		reply.Success = true
	}

	reply.Term = rf.currentTerm
	log.Printf("[%v] AppendEntriesReply sent: %+v", rf.me, reply)
	return nil
}

// AppendEntriesArgs arguments sent in AppendEntry() RPC
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	//LastLogIndex int // index of candidate’s last log entry
	//LastLogTerm  int // term of candidate’s last log entry
}

// AppendEntriesReply results from AppendEntry() RPC
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote is invoked by candidates to gather votes
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%v] received RequestVote: %+v [currentTerm=%d, votedFor=%d]",
		rf.me, args, rf.currentTerm, rf.votedFor)
	if rf.state == Down {
		return nil
	}
	if args.Term > rf.currentTerm {
		// server in past term, revert to follower (and reset its state)
		log.Printf("[%v] RequestVoteArgs.Term=%d bigger than currentTerm=%d",
			rf.me, args.Term, rf.currentTerm)
		rf.toFollower(args.Term)
	}

	if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElection = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	log.Printf("[%v] RequestVoteReply: %+v", rf.me, reply)
	return nil
}
