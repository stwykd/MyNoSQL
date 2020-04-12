package raft

import (
	"math/rand"
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
			//rf.election()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}
