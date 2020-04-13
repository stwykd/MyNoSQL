package raft

import (
	"math/rand"
	"net/rpc"
	"sync/atomic"
	"time"
)


const minElectionWait, maxElectionWait = 150, 300

// electionWait starts a timer towards becoming a candidate in a new election.
// it is run by a follower as long as it receives heartbeats
func (rf *Raft) electionWait() {
	waitTimeout := time.Duration(rand.Intn(maxElectionWait)-minElectionWait) * time.Millisecond
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		rf.mu.Lock()
		if rf.state != Candidate && rf.state != Follower {
			rf.mu.Unlock()
			return
		}

		if termStarted != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		if elapsed := time.Since(rf.resetElection); elapsed >= waitTimeout {
			rf.election()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// election starts a new election with this server as a candidate.
// Expects rf.mu to be locked.
func (rf *Raft) election() {
	rf.state = Candidate
	rf.currentTerm++
	savedCurrentTerm := rf.currentTerm
	rf.resetElection = time.Now()
	rf.votedFor = rf.me

	var votes int32 = 1
	for _, peer := range rf.cluster {
		go func(peer *rpc.Client) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: rf.me,
			}

			var reply RequestVoteReply
			if err := peer.Call("Raft.RequestVote", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate {
					return
				}

				if reply.Term > savedCurrentTerm {
					rf.toFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votes, 1))
						if votes*2 > len(rf.cluster)+1 { // election won
							rf.leader()
							return
						}
					}
				}
			}
		}(peer)
	}

	// this election didn't produce a leader, call electionWait again
	go rf.electionWait()
}