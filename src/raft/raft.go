package raft

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Raft Consensus Algorithm implemented as a library
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// Raft for a single Raft server
type Raft struct {
	me            int        // id of this Raft server
	mu            sync.Mutex // mutex to protect shared access and ensure visibility
	RPCServer                // handles RPC communication to Raft peers
	state         State      // state of this Raft server
	logIndex      int        // log index where to store next log entry
	resetElection time.Time  // time (used by follower) to wait before starting election
	peers         []int      // Raft peers (not including this server)

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
func NewRaft(me int, peers []int) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.peers = peers
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower // all servers start as follower
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.logIndex = 1
	rf.resetElection = time.Now()

	rf.clients = make(map[int]*rpc.Client)
	rf.server = rpc.NewServer()
	if err := rf.server.RegisterName("Raft", rf); err != nil {
		log.Fatalf("[%v] unable to register RPC Server", rf.me)
	}
	var err error
	if rf.listener, err = net.Listen("tcp", ":0"); err != nil {
		log.Fatalf("[%v] unable to initialize listener: %s", rf.me, err.Error())
	}
	log.Printf("[%v] listening at %s", rf.me, rf.listener.Addr())

	rf.recover() // initialize from saved state before possible server crash
	go rf.electionWait()

	return rf
}

// toFollower changes Raft server state to follower and resets its state
// Expects rf.mu to be locked
func (rf *Raft) toFollower(term int) {
	log.Printf("[%v] becoming follower with term %v", rf.me, term)
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

	for _, id := range rf.peers {
		args := AppendEntriesArgs{
			Term:     savedTerm,
			LeaderId: rf.me,
			Recipient: id,
		}
		go func(id int) {
			log.Printf("[%v] sending AppendEntries to %v: args=%+v", rf.me, id, args)
			var reply AppendEntriesReply
			if err := Call(rf, id, "Raft.AppendEntries", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > savedTerm {
					log.Printf("[%v] received heartbeat reply with higher term, becoming follower",
						rf.me)
					rf.toFollower(reply.Term)
					return
				}
			}
		}(id)
	}
}

// toLeader changes Raft server into leader state and starts sending of heartbeats
// Expects rf.mu to be locked
func (rf *Raft) toLeader() {
	rf.state = Leader
	log.Printf("[%v] becoming leader at term %v", rf.me, rf.currentTerm)

	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()

		// send periodic heartbeats as long as still leader
		for {
			rf.heartbeat()
			<-ticker.C

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}
