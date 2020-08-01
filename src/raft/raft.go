package raft

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Raft Consensus Algorithm implemented as a library
// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

const HeartbeatInterval = 50 * time.Millisecond

// LogEntry is a single entry in a Raft server's log
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// State of a Raft server
type State string

const (
	Leader    State = "leader"
	Follower        = "follower"
	Candidate       = "candidate"
	Down            = "down" // for testing
)

// Commit is used to clientCh the client that a command was replicated by
// a majority (ie it was committed) and it can now be applied by client
type Commit struct {
	Index   int // log index
	Term    int
	Command interface{}
}

type RPCServer struct {
	server   *rpc.Server
	clients  map[int]*rpc.Client
	listener net.Listener
}

type Raft struct {
	me            int        // id of this Raft server
	mu            sync.Mutex // mutex to protect shared access and ensure visibility
	RPCServer                // handles RPC communication to Raft peers
	server        *Server
	leader 		  int
	state         State      // state of this Raft server
	logIndex      int        // log index where to store next log entry
	resetElection time.Time  // time to wait before starting election. used by followers
	peers         []int      // Raft peers (not including this server)

	clientCh chan <-Commit   // notify the client app when commands are committed
	readyCh  chan struct{}   // Use to signal when new entries are ready to be sent to client

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
func NewRaft(id int, peerIds []int, server *Server, storage Storage, ready <-chan interface{}, clientCh chan <-Commit) *Raft {
	rf := new(Raft)
	rf.me = id
	rf.peers = peerIds
	rf.server = server
	rf.storage = storage
	rf.clientCh = clientCh
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
	log.Printf("[%d] stopped", rf.me)
	close(rf.readyCh)
}

// range suggested in paper
const minElectionWait, maxElectionWait = 150, 300

// electionWait starts a timer towards becoming a candidate in a new election.
// it is run by a follower as long as it receives heartbeats
func (rf *Raft) electionWait() {
	waitTimeout := time.Duration(rand.Intn(maxElectionWait-minElectionWait)+minElectionWait) * time.Millisecond
	rf.mu.Lock()
	termStart := rf.currentTerm
	rf.mu.Unlock()
	log.Printf("[%v] electionWait() started: timeout=%v term=%v", rf.me, waitTimeout, termStart)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		rf.mu.Lock()
		// election() running concurrently. server may be a candidate or even a leader already
		if rf.state != Candidate && rf.state != Follower {
			log.Printf("[%v] waiting for election with state=%v instead of follower, return",
				rf.me, rf.state)
			rf.mu.Unlock()
			return
		}
		if termStart != rf.currentTerm {
			log.Printf("[%v] while waiting for election, term changed from %d to %d, return",
				rf.me, termStart, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		if elapsed := time.Since(rf.resetElection); elapsed >= waitTimeout {
			log.Printf("[%v] reset timer elapsed: %s", rf.me, elapsed.String())
			rf.election()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// election starts a new election with this server as a candidate.
// Expects rf.mu to be locked.
func (rf *Raft) election() {
	rf.state = Candidate
	rf.currentTerm++
	savedCurrentTerm := rf.currentTerm
	rf.resetElection = time.Now()
	log.Printf("[%v] started election and became candidate at term %v", rf.me, savedCurrentTerm)

	// candidate votes for itself
	rf.votedFor = rf.me
	var votes int32 = 1

	for _, peer := range rf.peers {
		go func(peer int) {
			rf.mu.Lock()
			lastLogIndex, lastLogTerm := rf.lastLogIdxAndTerm()
			rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			log.Printf("[%v] sending RequestVote to %d: Args%+v", rf.me, peer, args)

			var reply RequestVoteReply
			if err := rf.server.Call(peer, "Raft.RequestVote", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				log.Printf("[%v] received RequestVoteReply %+v", rf.me, reply)
				if rf.state != Candidate {
					// might have won election already because there were enough votes from
					// the other concurrent RequestVote calls issued, or one reply had higher
					// term, and switched back to follower. return from election
					log.Printf("[%v] state changed to %v while waiting for RequestVoteReply",
						rf.me, rf.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					// reply term higher than saved term. this can happen if another candidate won
					// an election while we were collecting votes
					log.Printf("[%v] RequestVoteReply.Term=%v while currentTerm=%v, returning",
						rf.me, reply.Term, savedCurrentTerm)
					rf.toFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votes, 1))
						if votes > (len(rf.peers)+1)/2 {
							// election won. become leader
							// remaining goroutines will notice state != candidate and return
							log.Printf("[%v] received %d votes, becoming leader", rf.me, votes)
							rf.toLeader()
							return
						}
					}
				}
			} else {
				log.Printf("[%v] error during RequestVote RPC: %s", rf.me, err.Error())
			}
		}(peer)
	}

	// wait to start another election
	go rf.electionWait()
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

func (rf *Raft) lastLogIdxAndTerm() (int, int) {
	if len(rf.log) > 0 { // log starts at 1
		lastIdx := len(rf.log) - 1
		return lastIdx, rf.log[lastIdx].Term
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
	rf.leader = rf.me

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

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}