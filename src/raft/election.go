package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"sync/atomic"
	"time"
)

// range suggested in paper
const minElectionWait, maxElectionWait = 150, 300

// electionWait starts a timer towards becoming a candidate in a new election.
// it is run by a follower as long as it receives heartbeats
func (rf *Raft) electionWait() {
	waitTimeout := time.Duration(rand.Intn(maxElectionWait)-minElectionWait) * time.Millisecond
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()
	DPrintf("[%v] electionWait() started: timeout=%v term=%v", rf.me, waitTimeout, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		rf.mu.Lock()
		if rf.state != Candidate && rf.state != Follower {
			log.Printf("[%v] waiting for election with state=%v, return", rf.me, rf.state)
			rf.mu.Unlock()
			return
		}

		if termStarted != rf.currentTerm {
			log.Printf("[%v] while waiting for election, term changed from %d to %d, return",
				rf.me, termStarted, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

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
	log.Printf("[%v] started election and become Candidate at term %v", rf.me, savedCurrentTerm)

	// candidate votes for itself
	rf.votedFor = rf.me
	var votes int32 = 1
	for id, peer := range rf.cluster {
		go func(id int, peer *rpc.Client) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: rf.me,
			}
			log.Printf("[%v] sending RequestVote to %d: %+v", rf.me, id, args)
			var reply RequestVoteReply
			// blocking RPC call
			if err := peer.Call("Raft.RequestVote", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				log.Printf("[%v] received RequestVoteReply %+v", rf.me, reply)
				if rf.state != Candidate {
					// might have won election already because there were enough votes from
					// the other concurrent RequestVote calls issued, or one reply had higher
					// term, and switched back to Follower. return from election
					log.Printf("[%v] state changed to %v while waiting for RequestVoteReply",
						rf.me, rf.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					// reply term higher than saved term. this can happen if another candidate won
					// an election while we were collecting votes
					log.Printf("[%v] RequestVoteReply.Term=%v while currentTerm=%v, returning",
						rf.me, reply.Term, savedCurrentTerm)
					rf.toFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votes, 1))
						if votes*2 > len(rf.cluster)+1 { // election won
							log.Printf("[%v] received %d votes, becoming leader", rf.me, votes)
							rf.toLeader()
							return
						}
					}
				}
			} else {
				log.Printf("[%v] error during RequestVote RPC: %s", rf.me, err.Error())
			}
		}(id, peer)
	}

	// this election didn't produce a leader, call electionWait again
	go rf.electionWait()
}