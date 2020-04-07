package raft

import (
	"net/rpc"
	"sync"
	"time"
)

// Raft Consensus Algorithm implemented as a library
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

// Raft for a single Raft server
type Raft struct {
	me          int           // id of this Raft server
	leader      int           // id of leader
	cluster     []*rpc.Client // RPC endpoint of each server in Raft cluster (including this one)
	mu          sync.Mutex    // mutex to protect shared access and ensure visibility
	timer       *time.Timer   // time to wait before starting election
	role        Role          // role of this Raft server
	doneCh      chan DoneMsg  // communicate back to Raft client when a command is committed into log
	logIndex    int           // log index where to store next log entry
	snapshotIdx int           // index of the last entry within log replaced by snapshot, initialized to 0
	notifyCh    chan struct{} // notify Raft client

	// State from Figure 2 of Raft paper
	// Persistent state on all servers
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex    []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex   []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func (r *Raft) apply() {
	for {
		select {
		case <-r.notifyCh:
			r.mu.Lock()
			var entries []LogEntry
			if r.lastApplied < r.snapshotIdx {
				r.lastApplied = r.snapshotIdx
				entries = []LogEntry{{Index: r.snapshotIdx, Term: r.log[0].Term, Command: "InstallSnapshot"}}
			} else if r.lastApplied < r.logIndex && r.lastApplied < r.commitIndex {
				entries = append([]LogEntry{}, r.log[r.lastApplied+1- r.snapshotIdx:r.commitIndex+1- r.snapshotIdx]...)
				r.lastApplied = r.commitIndex
			}
			r.saveState()
			r.mu.Unlock()
			for _, entry := range entries {
				r.doneCh <- DoneMsg{Index: entry.Index, Term: entry.Term, Command: entry.Command}
			}
		}
	}
}

// New initializes a new Raft server as of Figure 2 of Raft paper
func New(cluster []*rpc.Client, me int, doneCh chan DoneMsg) *Raft {
	r := &Raft{}
	r.cluster = cluster
	r.me = me
	r.leader = -1
	r.currentTerm = 0
	r.votedFor = -1
	r.commitIndex = 0
	r.lastApplied = 0
	r.role = Follower // all nodes start as follower
	r.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	r.logIndex = 1
	r.doneCh = doneCh
	r.notifyCh = make(chan struct{}, 100)
	r.snapshotIdx = 0
	r.timer = randElectionTimer()

	r.loadState() // initialize from saved state before possible server crash
	go r.startTimer()

	return r
}
