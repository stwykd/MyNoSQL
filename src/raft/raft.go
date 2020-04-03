package raft

import (
	"net/rpc"
	"sync"
	"time"
)

// Raft Consensus Algorithm implemented as a library
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

// State for a single Raft server
type State struct {
	me int // id of this Raft server
	leader int // id of leader
	cluster []*rpc.Server // RPC endpoint of each server in Raft cluster (including this one)
	mu sync.Mutex // mutex to protect shared access and ensure visibility
	electionTimer *time.Timer // time to wait before starting election
	role Role // role of this Raft server
	doneCh chan DoneMsg // communicate back to Raft client when a command is committed into log
	logIndex int // log index where to store next log entry

	// State from Figure 2 of Raft paper
	// Persistent state on all servers
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int // candidateId that received vote in current term (or null if none)
	log []LogEntry // each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}



// New initializes a new Raft server as of Figure 2 of Raft paper
func New(cluster []*rpc.Server, me int, doneCh chan DoneMsg) *State {
	s := &State{}
	s.cluster = cluster
	s.me = me
	s.leader = -1
	s.currentTerm = 0
	s.votedFor = -1
	s.commitIndex = 0
	s.lastApplied = 0
	s.role = Follower // all nodes start as follower
	s.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	s.logIndex = 1
	s.doneCh = doneCh
	s.electionTimer = randElectionTimer()
	return s
}