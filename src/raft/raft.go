package raft

import (
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Raft Consensus Algorithm implemented as a library
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type Raft struct {
	me            int        // id of this Raft server
	mu            sync.Mutex // mutex to protect shared access and ensure visibility
	RPCServer                // handles RPC communication to Raft peers
	server   *TestServer
	state         State      // state of this Raft server
	logIndex      int        // log index where to store next log entry
	resetElection time.Time  // time to wait before starting election. used by follower
	peers         []int      // Raft peers (not including this server)

	clientCh chan <-Commit   // notify the client app when commands are committed
	readyCh  chan struct{} // Use internally to signal when new entries are ready to be sent  to the client

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
	matchIndex map[int]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	storage Storage // storage is an interface for disk storage used to persist Raft state. it simplifies testing
	updated chan struct{} // updated is used by leaders in heartbeat() to know when the internal state changes
}

// NewRaft initializes a new Raft server as of Figure 2 of Raft paper
func NewRaft(id int, peerIds []int, server *TestServer, storage Storage, ready <-chan interface{}, commitChan chan <-Commit) *Raft {
	rf := new(Raft)
	rf.me = id
	rf.peers = peerIds
	rf.server = server
	rf.storage = storage
	rf.clientCh = commitChan
	rf.readyCh = make(chan struct{}, 16)
	rf.updated = make(chan struct{}, 1)
	rf.state = Follower
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	if rf.storage.Ready() {
		rf.restore()
	}

	go func() {
		<-ready
		rf.mu.Lock()
		rf.resetElection = time.Now()
		rf.mu.Unlock()
		rf.electionWait()
	}()

	go rf.notifyClient()
	return rf
}

// Stop stops this Raft server
func (rf *Raft) Stop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Down
	log.Printf("becomes Down")
	close(rf.readyCh)
}

// electionTimeout generates a pseudo-random election timeout duration.
func (rf *Raft) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
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
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for _, peerId := range rf.peers {
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
				Term:         currentTerm,
				Leader:       rf.me,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			log.Printf("[%v] sending AppendEntries to %v: args=%+v", rf.me, peer, args)
			var reply AppendEntriesReply
			if err := rf.server.Call(peer, "Raft.AppendEntries", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > currentTerm {
					log.Printf("[%v] received heartbeat reply with higher term, becoming follower", rf.me)
					rf.toFollower(reply.Term)
					return
				}

				if rf.state == Leader && currentTerm == reply.Term {
					// reply.Success communicates to the leader whether the follower had same prevLogIdx
					// and prevLogTerm. The leader uses reply.Success to update nextIndex for the follower
					if reply.Success {
						rf.nextIndex[peer] = nextIdx + len(entries)
						// peer appended new entries update matchIndex for this peer
						rf.matchIndex[peer] = rf.nextIndex[peer] - 1
						log.Printf("[%v] AppendEntriesReply from %d successful (nextIndex:%v, matchIndex:%v)",
							rf.me, peer, rf.nextIndex, rf.matchIndex)
						commitIdx := rf.commitIndex
						for i := rf.commitIndex + 1; i < len(rf.log); i++ {
							if rf.log[i].Term == rf.currentTerm {
								matches := 1
								for _, peerId := range rf.peers {
									if rf.matchIndex[peerId] >= i {
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
						if rf.commitIndex != commitIdx {
							log.Printf("leader sets commitIndex := %d", rf.commitIndex)
							rf.readyCh <- struct{}{}
							rf.updated <- struct{}{}
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIdxTerm := -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									lastIdxTerm = i
									break
								}
							}
							if lastIdxTerm >= 0 {
								rf.nextIndex[peer] = lastIdxTerm + 1
							} else {
								rf.nextIndex[peer] = reply.ConflictIndex
							}
						} else {
							rf.nextIndex[peer] = reply.ConflictIndex
						}
						log.Printf("[%v] AppendEntriesReply from %d unsuccessful (nextIndex: %d)", rf.me, peer, nextIdx-1)
					}
				}
			}
		}(peerId)
	}
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log) - 1
		return lastIndex, rf.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// notifyClient notifies the client of committed log entries. Those are command which were
// sent by client using Replicate() and are now replicated amongst the majority
// It updates lastApplied marking which entries were sent already, and sends any new entries
func (rf *Raft) notifyClient() {
	for range rf.readyCh {
		rf.mu.Lock()
		term := rf.currentTerm
		lastApplied := rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		log.Printf("[%v] notifying client of new entries %+v, lastApplied %d", rf.me, entries, lastApplied)

		for i, e := range entries {
			rf.clientCh <- Commit{lastApplied + i + 1, term, e.Command}
		}
	}
	log.Printf("[%v] client notified", rf.me)
}



// toLeader changes Raft server into leader state and starts sending of heartbeats
// Expects rf.mu to be locked
func (rf *Raft) toLeader() {
	rf.state = Leader

	for _, peerId := range rf.peers {
		rf.nextIndex[peerId] = len(rf.log)
		rf.matchIndex[peerId] = -1
	}

	log.Printf("[%v] becoming leader at term %v", rf.me, rf.currentTerm)

	go func() {
		rf.heartbeat()

		t := time.NewTimer(HeartbeatInterval)
		defer t.Stop()
		for {
			send := false
			// send heartbeat if leader state is updated or 50ms is elapsed. in either case, reset heartbeat timer
			select {
			case <-t.C:
				send = true

				t.Stop()
				t.Reset(HeartbeatInterval)
			case _, ok := <-rf.updated:
				if ok {
					send = true
				} else {
					return
				}

				if !t.Stop() {
					<-t.C
				}
				t.Reset(HeartbeatInterval)
			}

			if send {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.heartbeat()
			}
		}
	}()
}

// Replicate is called by client on the leader to append a new command
// to the leader's log. the leader will then replicate it to its peers.
// once the command is committed, the leader will use clientCh to notify
// the client
func (rf *Raft) Replicate(cmd interface{}) bool {
	rf.mu.Lock()
	log.Printf("[%v] Replicate() called by client with %v", rf.me, cmd)

	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: cmd, Term: rf.currentTerm})
		rf.persist()
		log.Printf("[%v] log %+v", rf.me, rf.log)
		rf.mu.Unlock()
		rf.updated <- struct{}{}
		return true
	}

	rf.mu.Unlock()
	return false
}