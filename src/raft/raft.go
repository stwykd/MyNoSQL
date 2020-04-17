package raft

import (
	"log"
	"net/rpc"
	"sync"
	"time"
)

// Raft Consensus Algorithm implemented as a library
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

// Raft for a single Raft server
type Raft struct {
	me            int         // id of this Raft server
	leaderId      int         // id of leader
	cluster       Cluster     // endpoint of each other server in Raft cluster (excluding this one)
	mu            sync.Mutex  // mutex to protect shared access and ensure visibility
	timer         *time.Timer // time to wait before starting election
	state         State       // state of this Raft server
	logIndex      int         // log index where to store next log entry
	resetElection time.Time   // time (used by follower) to wait before starting election

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

// Kill kills the this Raft server. Used for testing
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Down
	log.Printf("[%v] killed", rf.me)
}

// NewRaft initializes a new Raft server as of Figure 2 of Raft paper
func NewRaft(cluster Cluster, me int, doneCh chan DoneMsg) *Raft {
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

	rf.recover() // initialize from saved state before possible server crash
	go rf.electionWait()

	return rf
}

// toFollower changes Raft server state to follower and resets its state
// Expects rf.mu to be locked
func (rf *Raft) toFollower(term int) {
	log.Printf("[%v] becoming Follower with term=%v", rf.me, term)
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
	savedTerm := rf.currentTerm
	rf.mu.Unlock()

	for id, peer := range rf.cluster {
		args := AppendEntriesArgs{
			Term:     savedTerm,
			LeaderId: rf.me,
		}
		go func(id int, peer *rpc.Client) {
			log.Printf("[%v] sending AppendEntries to %v: args=%+v", rf.me, id, args)
			var reply AppendEntriesReply
			if err := peer.Call("Raft.AppendEntries", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > savedTerm {
					log.Printf("[%v] received heartbeat reply with higher term, becoming follower",
						rf.me)
					rf.toFollower(reply.Term)
					return
				}
			}
		}(id, peer)
	}
}

// toLeader changes Raft server into leader state and starts sending of heartbeats
// Expects rf.mu to be locked
func (rf *Raft) toLeader() {
	rf.state = Leader
	log.Printf("[%v] becoming Leader at term %v", rf.me, rf.currentTerm)
	timer := time.NewTimer(HeartbeatInterval)
	for {
		select {
		case <-timer.C:
			if rf.state != Leader {
				return
			}
			rf.heartbeat()
			timer.Reset(HeartbeatInterval)
		}
	}
}