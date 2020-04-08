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
	state       State         // state of this Raft server
	logIndex    int           // log index where to store next log entry

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
	r.state = Follower              // all nodes start as follower
	r.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	r.logIndex = 1
	r.timer = randElectionTimer()

	r.loadState() // initialize from saved state before possible server crash
	go r.runElectionTimer()

	return r
}

// toFollower turns Raft server into follower
// Expects r.mu to be acquired
func (r *Raft) toFollower(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = -1

	go r.runElectionTimer()
}