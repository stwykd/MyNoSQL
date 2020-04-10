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
	me            int           // id of this Raft server
	leaderId      int           // id of leader
	cluster       []*rpc.Client // RPC endpoint of each server in Raft cluster (including this one)
	mu            sync.Mutex    // mutex to protect shared access and ensure visibility
	timer         *time.Timer   // time to wait before starting election
	state         State         // state of this Raft server
	logIndex      int           // log index where to store next log entry
	resetElection time.Time

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
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// NewRaft initializes a new Raft server as of Figure 2 of Raft paper
func NewRaft(cluster []*rpc.Client, me int, doneCh chan DoneMsg) *Raft {
	rf := &Raft{}
	rf.cluster = cluster
	rf.me = me
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower              // all servers start as follower
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.logIndex = 1
	rf.resetElection = time.Now()

	rf.loadState() // initialize from saved state before possible server crash
	go rf.electionWait()

	return rf
}

// toFollower changes Raft server state to follower
// Expects rf.mu to be acquired
func (rf *Raft) toFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElection = time.Now()
	go rf.electionWait()
}

// heartbeat is run by leader. it sends heartbeats to each follower, collects
// their replies and reverts to follower if any follower has higher term
func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()

	for _, peer := range rf.cluster {
		args := AppendEntriesArgs{
			Term:     term,
			LeaderId: rf.me,
		}
		go func(peer *rpc.Client) {
			var reply AppendEntriesReply
			if err := peer.Call("Raft.AppendEntries", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > term {
					rf.toFollower(reply.Term)
					return
				}
			}
		}(peer)
	}
}
