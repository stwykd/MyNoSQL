package raft

import (
	"fmt"
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
	RPCServer           // handles RPC communication to Raft peers
	state         State      // state of this Raft server
	logIndex      int        // log index where to store next log entry
	resetElection time.Time  // time (used by follower) to wait before starting election
	peers         []int      // Raft peers (not including this server)

	commitCh chan CommitMsg // notify the client app when commands are committed
	readyCh  chan struct{}    // Use internally to signal when new entries are ready to be sent  to the client

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
	nextIndex  map[int]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// NewRaft initializes a new Raft server as of Figure 2 of Raft paper
func NewRaft(me int, peers []int, commitCh chan CommitMsg) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.peers = peers
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower // all servers start as follower
	rf.log = []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.logIndex = 1
	rf.resetElection = time.Now()
	rf.commitCh = commitCh
	rf.readyCh = make(chan struct{}, 16)
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

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

	for _, peer := range rf.peers {
		go func(peer int) {
			rf.mu.Lock()
			nextIdx := rf.nextIndex[peer]
			prevLogIdx := nextIdx - 1
			prevLogTerm := -1
			if prevLogIdx >= 0 {
				prevLogTerm = rf.log[prevLogIdx].Term
			}
			entries := rf.log[nextIdx:]
			args := AppendEntriesArgs{
				Term:         savedTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			log.Printf("[%v] sending AppendEntries to %v: args=%+v", rf.me, id, args)
			var reply AppendEntriesReply
			if err := Call(rf, peer, "Raft.AppendEntries", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > savedTerm {
					log.Printf("[%v] received heartbeat reply with higher term, becoming follower",
						rf.me)
					rf.toFollower(reply.Term)
					return
				}

				if rf.state == Leader && savedTerm == reply.Term {
					// reply.Success communicates to the leader whether the follower had same prevLogIdx
					// and prevLogTerm. The leader uses reply.Success to update nextIndex for the follower
					if reply.Success {
						rf.nextIndex[peer] = nextIdx + len(entries)
						// peer appended new entries update matchIndex for this peer
						rf.matchIndex[peer] = rf.nextIndex[peer] - 1
						fmt.Printf("[%v] AppendEntries reply from %d success: nextIndex:%v, matchIndex:%v",
							rf.me, peer, rf.nextIndex, rf.matchIndex)

						savedCommitIdx := rf.commitIndex
						for i := rf.commitIndex + 1; i < len(rf.log); i++ {
							if rf.log[i].Term == rf.currentTerm {
								matches := 1
								for _, peer := range rf.peers {
									if rf.matchIndex[peer] >= i {
										matches++
									}
								}
								// if i is replicated by majority, commitIndex advances to i
								// the new commitIndex will be sent to follower in the next AppendEntries() RPC
								if matches*2 > len(rf.peers)+1 {
									rf.commitIndex = i
								}
							}
						}
						if rf.commitIndex != savedCommitIdx {
							fmt.Printf("[%v] leader set commitIndex to %d", rf.me, rf.commitIndex)
							rf.readyCh <- struct{}{}
						}
					} else {
						rf.nextIndex[peer] = nextIdx - 1
						fmt.Printf("[%v] AppendEntries reply from %d !success: nextIndex:%d",
							rf.me, peer, nextIdx-1)
					}
				}
			}
		}(peer)
	}
}

// NotifyCommit sends committed entries to the client
// it updates lastApplied to know which entries were sent already, and sends any new entries
func (rf *Raft) NotifyCommit() {
	for range rf.readyCh {
		// Find which entries we have to apply.
		rf.mu.Lock()
		savedTerm := rf.currentTerm
		savedLastApplied := rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		fmt.Printf("NotifyCommit entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			msg := CommitMsg{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
			rf.commitCh<-msg
		}
	}
	fmt.Printf("NotifyCommit done")
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

// Replicate is called by client on the leader to append a new command
// to the leader's log. the leader will then replicate it to its peers.
// once the command is committed, the leader will use commitCh to notify
// the client
func (rf *Raft) Replicate(command interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[%v] Replicate() called by client with %v", rf.me, command)
	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		fmt.Printf("[%v] log: %v", rf.me, rf.log)
		return true
	}
	return false
}
