package raft

// Raft Consensus Algorithm implemented as of paper https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

type LogEntry struct {
	Index int
	Term  int
	Command  interface{}
}

type State struct { // State from Figure 2 of paper
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
