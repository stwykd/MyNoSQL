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
	resetElection time.Time  // time to wait before starting election. used by follower
	peers         []int      // Raft peers (not including this server)

	clientCh chan Commit   // notify the client app when commands are committed
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
func NewRaft(me int, peers []int, clientCh chan Commit, storage Storage) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.peers = peers
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower // all servers start as follower
	rf.logIndex = 1
	rf.clientCh = clientCh
	rf.readyCh = make(chan struct{}, 16)
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.clients = make(map[int]*rpc.Client)
	rf.storage = storage
	rf.updated = make(chan struct{}, 1)

	if rf.storage.Ready() { // Raft server previously crashed and can be restored
		rf.restore()
	}

	rf.server = rpc.NewServer()
	if err := rf.server.RegisterName("Raft", rf); err != nil {
		log.Fatalf("[%v] unable to register RPC Server", rf.me)
	}
	var err error
	if rf.listener, err = net.Listen("tcp", ":0"); err != nil {
		log.Fatalf("[%v] unable to initialize listener: %s", rf.me, err.Error())
	}
	log.Printf("[%v] listening at %s", rf.me, rf.listener.Addr())

	go func(){
		rf.mu.Lock()
		rf.resetElection = time.Now()
		rf.mu.Unlock()
		rf.electionWait()
	}()

	go NotifyClient(rf)
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
	term := rf.currentTerm
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
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			log.Printf("[%v] sending AppendEntries to %v: args=%+v", rf.me, peer, args)
			var reply AppendEntriesReply
			if err := Call(rf, peer, "Raft.AppendEntries", args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > term {
					log.Printf("[%v] received heartbeat reply with higher term, becoming follower",
						rf.me)
					rf.toFollower(reply.Term)
					return
				}

				if rf.state == Leader && term == reply.Term {
					// reply.Success communicates to the leader whether the follower had same prevLogIdx
					// and prevLogTerm. The leader uses reply.Success to update nextIndex for the follower
					if reply.Success {
						rf.nextIndex[peer] = nextIdx + len(entries)
						// peer appended new entries update matchIndex for this peer
						rf.matchIndex[peer] = rf.nextIndex[peer] - 1
						log.Printf("[%v] AppendEntries reply from %d success: nextIndex:%v, matchIndex:%v",
							rf.me, peer, rf.nextIndex, rf.matchIndex)

						commitIdx := rf.commitIndex
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
						if rf.commitIndex != commitIdx {
							log.Printf("[%v] leader set commitIndex to %d", rf.me, rf.commitIndex)
							rf.readyCh <- struct{}{}
							rf.updated <- struct{}{}
						}
					} else {
						rf.nextIndex[peer] = nextIdx - 1
						log.Printf("[%v] AppendEntries reply from %d !success: nextIndex:%d",
							rf.me, peer, nextIdx-1)
					}
				}
			}
		}(peer)
	}
}

// NotifyClient notifies the client of committed log entries. Those are command which were
// sent by client using Replicate() and are now replicated amongst the majority
// It updates lastApplied marking which entries were sent already, and sends any new entries
func NotifyClient(rf *Raft) {
	for range rf.readyCh {
		rf.mu.Lock()
		term, lastApplied := rf.currentTerm, rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		log.Printf("[%v] notifying client of new entries %v, lastApplied %d", rf.me, entries, lastApplied)

		for i, entry := range entries {
			rf.clientCh <-Commit{
				Command: entry.Command,
				Index:   lastApplied + i + 1,
				Term:    term,
			}
		}
	}
	log.Printf("[%v] client notified", rf.me)
}

// toLeader changes Raft server into leader state and starts sending of heartbeats
// Expects rf.mu to be locked
func (rf *Raft) toLeader() {
	rf.state = Leader

	// TODO
	//for _, peer := range rf.peers {
	//	rf.nextIndex[peer] = len(rf.log)
	//	rf.matchIndex[peer] = -1
	//}

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
				// TODO remove send
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
func Replicate(rf *Raft, cmd interface{}) bool {
	rf.mu.Lock()
	log.Printf("[%v] Replicate() called by client with %v", rf.me, cmd)

	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: cmd, Term: rf.currentTerm})
		rf.persist()
		log.Printf("[%v] log %v", rf.me, rf.log)
		rf.mu.Unlock()
		rf.updated <- struct{}{}
		return true
	}

	rf.mu.Unlock()
	return false
}