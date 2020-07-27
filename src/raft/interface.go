package raft

import (
	"log"
	"time"
)

// RPC interface exposed by each Raft server
// See Figure 2 of Raft paper
// RPC calls can take a long while to arrive - when replying, the code may have moved on
// and it's important to gracefully give up in such cases.

// AppendEntriesArgs arguments sent in AppendEntry() RPC
type AppendEntriesArgs struct {
	Term   int // leader's term
	Leader int // so that followers can redirect peers to leader

	PrevLogIndex int // index log index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader’s commitIndex
}

// AppendEntriesReply results from AppendEntry() RPC
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// leader to take follower up to date more quickly (end section 5.3 of paper)
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries is invoked by leader to replicate log entries; also used as heartbeat
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Down {
		return nil
	}

	log.Printf("[%v] received AppendEntries RPC call: Args%+v", rf.me, args)
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

		// does follower log match leader's (-1 is valid)
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// merge follower's log with leader's log starting from args.PrevLogTerm
			// skip entries where the term matches where term matches with args.Entries
			// and insert args.Entries from mismatch index
			insertIdx, appendIdx := args.PrevLogIndex + 1, 0
			for {
				if insertIdx >= len(rf.log) || appendIdx >= len(args.Entries) {
					break
				}
				if rf.log[insertIdx].Term != args.Entries[appendIdx].Term {
					break
				}
				insertIdx++
				appendIdx++
			}
			// At the end of this loop:
			// - insertIdx points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - appendIdx points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if appendIdx < len(args.Entries) {
				log.Printf("[%v] append new entries %+v from %d", rf.me,
					args.Entries[appendIdx:], insertIdx)
				rf.log = append(rf.log[:insertIdx], args.Entries[appendIdx:]...)
				log.Printf("[%v] new log: %+v", rf.me, rf.log)
			}

			// update rf.commitIndex if the leader considers additional log entries as committed
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.log)-1
				}

				log.Printf("[%v] updated commitIndex:%d", rf.me, rf.commitIndex)
				rf.readyCh <- struct{}{}
			}
		} else {
			// PrevLogIndex and PrevLogTerm didn't match
			// set ConflictIndex and ConflictTerm to allow leader to send the right entries quickly
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictIndex = len(rf.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogTerm doesn't match
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
				var idx int
				for idx = args.PrevLogIndex - 1; idx >= 0; idx-- {
					if rf.log[idx].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = idx + 1
			}
		}
	}

	reply.Term = rf.currentTerm
	rf.persist()
	log.Printf("[%v] AppendEntriesReply sent: %+v", rf.me, reply)
	return nil
}

// RequestVoteArgs arguments sent in RequestVote() RPC
type RequestVoteArgs struct {
	Term         int // candidate's term
	Candidate    int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply results from RequestVote() RPC
type RequestVoteReply struct {
	Term        int // currentTerm, for candidate to update itself
	VoteGranted bool // did the candidate receive a vote?
}


// RequestVote is invoked by candidates to gather votes
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Down {
		return nil
	}
	lastLogIdx, lastLogTerm := rf.lastLogIndexAndTerm()
	log.Printf("[%v] received RequestVote RPC: %+v [currentTerm=%d votedFor=%d lastLogIdx=%d lastLogTerm=%d]",
		rf.me, args, rf.currentTerm, rf.votedFor, lastLogIdx, lastLogTerm)
	if args.Term > rf.currentTerm {
		// Raft server in past term, revert to follower (and reset its state)
		log.Printf("[%v] RequestVoteArgs.Term=%d bigger than currentTerm=%d",
			rf.me, args.Term, rf.currentTerm)
		rf.toFollower(args.Term)
	}

	// if hasn't voted or already voted for this candidate or
	// if the candidate has up-to-date log (section 5.4.1 from paper) ...
	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.Candidate) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx)) {
		// ... grant vote
		reply.VoteGranted = true
		rf.votedFor = args.Candidate
		rf.resetElection = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.persist()
	log.Printf("[%v] replying to RequestVote: %+v", rf.me, reply)
	return nil
}