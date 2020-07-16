package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

type TestServer struct {
	id    int
	peers []int

	rf      *Raft
	storage Storage

	rpcServer *rpc.Server
	listener  net.Listener

	clientCh chan<- Commit
	clients  map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}
	wg    sync.WaitGroup
}

func NewTestServer(id int, peers []int, storage Storage, ready <-chan interface{}, clientCh chan<- Commit) *TestServer {
	s := new(TestServer)
	s.id = id
	s.peers = peers
	s.clients = make(map[int]*rpc.Client)
	s.storage = storage
	s.ready = ready
	s.clientCh = clientCh
	s.quit = make(chan interface{})
	s.rf = NewRaft(s.id, s.peers, s, s.storage, s.ready, s.clientCh)
	return s
}

func (ts *TestServer) Run() {
	ts.rf.mu.Lock()

	ts.rpcServer = rpc.NewServer()
	if err := ts.rpcServer.RegisterName("Raft", ts.rf); err != nil {
		log.Fatal(err)
	}

	var err error
	ts.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %ts", ts.id, ts.listener.Addr())
	ts.rf.mu.Unlock()

	ts.wg.Add(1)
	go func() {
		defer ts.wg.Done()

		for {
			conn, err := ts.listener.Accept()
			if err != nil {
				select {
				case <-ts.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			ts.wg.Add(1)
			go func() {
				ts.rpcServer.ServeConn(conn)
				ts.wg.Done()
			}()
		}
	}()
}

func (ts *TestServer) GetListenAddr() net.Addr {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	return ts.listener.Addr()
}

// Connect connects test server to RPC client with id `id`
// requires mu to be acquired
func (ts *TestServer) Connect(peer int, addr net.Addr) error {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	if ts.clients[peer] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		ts.clients[peer] = client
	}
	return nil
}

// Disconnect disconnects RPC client with id `id` from test server
// requires mu to be acquired
func (ts *TestServer) Disconnect(peer int) error {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	if ts.clients[peer] != nil {
		err := ts.clients[peer].Close()
		ts.clients[peer] = nil
		return err
	}
	return nil
}

func (ts *TestServer) DisconnectPeers() {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	for id := range ts.clients {
		if ts.clients[id] != nil {
			if err := ts.clients[id].Close(); err != nil {
				log.Fatal(err)
			}
			ts.clients[id] = nil
		}
	}
}

func (ts *TestServer) Kill() {
	ts.rf.Stop()
	close(ts.quit)
	if err := ts.listener.Close(); err != nil {
		log.Fatal(err)
	}
	ts.wg.Wait()
}


func (ts *TestServer) Call(id int, method string, args interface{}, reply interface{}) error {
	ts.rf.mu.Lock()
	peer := ts.clients[id]
	ts.rf.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("client %d closed", id)
	} else {
		return peer.Call(method, args, reply)
	}
}


type TestCluster struct {
	mu        sync.Mutex
	cluster   []*TestServer
	storage   []*TestStorage
	clientChs []chan Commit
	commits   [][]Commit
	connected []bool
	alive     []bool
	n         int
	t         *testing.T
}

// NewTestCluster creates a new test TestCluster, initialized with n servers connected
// to each other
func NewTestCluster(t *testing.T, n int) *TestCluster {
	servers := make([]*TestServer, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	clientChs := make([]chan Commit, n)
	commits := make([][]Commit, n)
	ready := make(chan interface{})
	storage := make([]*TestStorage, n)

	for i := 0; i < n; i++ {
		peers := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peers = append(peers, p)
			}
		}

		storage[i] = NewTestStorage()
		clientChs[i] = make(chan Commit)
		servers[i] = NewTestServer(i, peers, storage[i], ready, clientChs[i])
		servers[i].Run()
		alive[i] = true
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				if err := servers[i].Connect(j, servers[j].GetListenAddr()); err != nil {
					log.Fatal(err)
				}
			}
		}
		connected[i] = true
	}
	close(ready)

	tc := &TestCluster{
		cluster:   servers,
		storage:   storage,
		clientChs: clientChs,
		commits:   commits,
		connected: connected,
		alive:     alive,
		n:         n,
		t:         t,
	}
	for i := 0; i < n; i++ {
		go tc.clientCommits(i)
	}
	return tc
}

// KillCluster shuts down all the servers in the harness and waits for them to
// stop running
func (tc *TestCluster) KillCluster() {
	for id := 0; id < tc.n; id++ {
		tc.cluster[id].DisconnectPeers()
		tc.connected[id] = false
	}
	for id := 0; id < tc.n; id++ {
		if tc.alive[id] {
			tc.alive[id] = false
			tc.cluster[id].Kill()
		}
	}
	for id := 0; id < tc.n; id++ {
		close(tc.clientChs[id])
	}
}

// Disconnect disconnects a server from all other servers in the cluster
func (tc *TestCluster) Disconnect(id int) {
	tc.cluster[id].DisconnectPeers()
	for j := 0; j < tc.n; j++ {
		if j != id {
			if err := tc.cluster[j].Disconnect(id); err != nil {
				log.Fatal(err)
			}
		}
	}
	tc.connected[id] = false
}

// Connect connects a server to all other servers in the cluster
func (tc *TestCluster) Connect(id int) {
	for j := 0; j < tc.n; j++ {
		if j != id && tc.alive[j] {
			if err := tc.cluster[id].Connect(j, tc.cluster[j].GetListenAddr()); err != nil {
				tc.t.Fatal(err)
			}
			if err := tc.cluster[j].Connect(id, tc.cluster[id].GetListenAddr()); err != nil {
				tc.t.Fatal(err)
			}
		}
	}
	tc.connected[id] = true
}

// == Election ==

func (tc *TestCluster) FindLeader() (int, int) {
	for r := 0; r < 8; r++ {
		leaderId, leaderTerm := -1, -1
		for i := 0; i < tc.n; i++ {
			if tc.connected[i] {
				tc.cluster[i].rf.mu.Lock()
				term, isLeader := tc.cluster[i].rf.currentTerm, tc.cluster[i].rf.state == Leader
				tc.cluster[i].rf.mu.Unlock()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						tc.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	tc.t.Fatalf("no leader elected")
	return -1, -1
}

// NoQuorum checks that no connected server considers itself the leader.
func (tc *TestCluster) NoQuorum() {
	for id := 0; id < tc.n; id++ {
		if tc.connected[id] {
			tc.cluster[id].rf.mu.Lock()
			isLeader := tc.cluster[id].rf.state == Leader
			tc.cluster[id].rf.mu.Unlock()
			if isLeader {
				tc.t.Fatalf("%d became a leader without quorum", tc.cluster[id].rf.me)
			}
		}
	}
}

// == Replication ==

func (tc *TestCluster) Committed(cmd int) (nc int, index int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	nCommits := -1
	for id := 0; id < tc.n; id++ {
		if tc.connected[id] {
			if nCommits >= 0 {
				if len(tc.commits[id]) != nCommits {
					tc.t.Fatalf("num commits sent to client differs amongst servers")
				}
			} else {
				nCommits = len(tc.commits[id])
			}
		}
	}

	for c := 0; c < nCommits; c++ {
		expCmd := -1
		for id := 0; id < tc.n; id++ {
			if tc.connected[id] {
				gotCmd := tc.commits[id][c].Command.(int)
				if expCmd >= 0 {
					if gotCmd != expCmd {
						tc.t.Errorf("got %d, want %d at tc.commits[%d][%d]", gotCmd, expCmd, id, c)
					}
				} else {
					expCmd = gotCmd
				}
			}
		}
		if expCmd == cmd {
			expIdx := -1
			nServers := 0
			for i := 0; i < tc.n; i++ {
				if tc.connected[i] {
					gotIdx := tc.commits[i][c].Index
					if expIdx >= 0 && gotIdx != expIdx {
						tc.t.Errorf("server %d committed idx %d for cmd %d whilst server %d committed idx %d"+
							" for the same cmd", 0, expIdx, cmd, i, gotIdx)
					} else {
						expIdx = tc.commits[i][c].Index
					}
					nServers++
				}
			}
			return nServers, expIdx
		}
	}

	tc.t.Errorf("command %d not amongst commits", cmd)
	return -1, -1
}

// CommittedN verifies that cmd was committed n times
func (tc *TestCluster) CommittedN(cmd int, n int) {
	if nCommitted, _ := tc.Committed(cmd); nCommitted != n {
		tc.t.Errorf("%d servers committed %d, expected %d servers to commit it", nCommitted, cmd, n)
	}
}

// NotCommitted verifies that cmd was not committed by any cluster server
func (tc *TestCluster) NotCommitted(cmd int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for id := 0; id < tc.n; id++ {
		if tc.connected[id] {
			for c := 0; c < len(tc.commits[id]); c++ {
				gotCmd := tc.commits[id][c].Command.(int)
				if gotCmd == cmd {
					tc.t.Errorf("found %d at commits[%d][%d], expected none", cmd, id, c)
				}
			}
		}
	}
}

func (tc *TestCluster) Replicate(id int, cmd interface{}) bool {
	return tc.cluster[id].rf.Replicate(cmd)
}

func (tc *TestCluster) clientCommits(id int) {
	log.Printf("[%v] start listening for commits", tc.cluster[id].rf.me)
	for c := range tc.clientChs[id] {
		tc.mu.Lock()
		tc.commits[id] = append(tc.commits[id], c)
		tc.mu.Unlock()
	}
}

// == Persist ==

func NewTestStorage() *TestStorage {
	return &TestStorage{st: make(map[string][]byte)}
}

func (ms *TestStorage) Get(k string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.st[k]
	return v, found
}

func (ms *TestStorage) Set(k string, v []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.st[k] = v
}

func (ms *TestStorage) Ready() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.st) > 0
}

// Crash crashes a server, however it's state is expected to be persisted
func (tc *TestCluster) Crash(id int) {
	log.Printf("[%v] server crashing", id)
	tc.Disconnect(id)
	tc.alive[id] = false
	tc.cluster[id].Kill()

	tc.mu.Lock()
	tc.commits[id] = tc.commits[id][:0]
	tc.mu.Unlock()
}

// Restart restarts previously crashed server
func (tc *TestCluster) Restart(id int) {
	if tc.alive[id] {
		log.Fatalf("me=%d is alive in Restart", id)
	}
	log.Printf("[%v] server starting", id)

	peerIds := make([]int, 0)
	for p := 0; p < tc.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan interface{})
	tc.cluster[id] = NewTestServer(id, peerIds, tc.storage[id], ready, tc.clientChs[id])
	tc.cluster[id].Run()
	tc.Connect(id)
	close(ready)
	tc.alive[id] = true
	time.Sleep(20*time.Millisecond)
}