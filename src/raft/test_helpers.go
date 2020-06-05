package raft

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

// slack durations to wait to allow things to settle
const ServerSlack  = 150 * time.Millisecond
const ClusterSlack = 450 * time.Millisecond

type TestServer struct {
	rf       *Raft
	commits  []Commit // commits sent to client by this server
	clientCh chan Commit // used to intercept commits sent to the client
	wg       sync.WaitGroup
	close	 chan interface{}
	t	     *testing.T
}

func NewServer(id int, peers []int, t *testing.T) *TestServer {
	ts := new(TestServer)
	ts.clientCh = make(chan Commit)
	ts.rf = NewRaft(id, peers, ts.clientCh)
	ts.commits = make([]Commit, 0)
	ts.t = t
	ts.close = make(chan interface{})
	return ts
}

func (ts *TestServer) Start() {
	ts.wg.Add(1)
	go func() {
		defer ts.wg.Done()

		for {
			conn, err := ts.rf.listener.Accept()
			if err != nil {
				select {
				case <-ts.close:
					return
				default:
					ts.t.Errorf("accept error: %s", err)
				}
			}
			ts.wg.Add(1)
			go func() {
				ts.rf.server.ServeConn(conn)
				ts.wg.Done()
			}()
		}
	}()
}

// Connect connects test server to RPC client with id `id`
// requires mu to be acquired
func (ts *TestServer) Connect(id int, addr net.Addr) {
	if ts.rf.clients[id] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			ts.t.Errorf("%d couldn't dial %d: %s", ts.rf.me, id, err.Error())
		}
		ts.rf.clients[id] = client
	}
}

// Disconnect disconnects RPC client with id `id` from test server
// requires mu to be acquired
func (ts *TestServer) Disconnect(id int) {
	if ts.rf.clients[id] != nil {
		if err := ts.rf.clients[id].Close(); err != nil {
			ts.t.Errorf("[%v] unable to close connection to %d: %s", ts.rf.me, id, err.Error())
		}
		ts.rf.clients[id] = nil
	}
}

// DisconnectPeers disconnects all RPC clients
func (ts *TestServer) DisconnectPeers() {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	for id := range ts.rf.clients {
		ts.Disconnect(id)
	}
}

// Kill closes the RPC server connections and marks it `Down`
func (ts *TestServer) Kill() {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	ts.rf.state = Down
	log.Printf("[%v] killed", ts.rf.me)

	close(ts.close)
	if err := ts.rf.listener.Close(); err != nil {
		ts.t.Errorf("[%v] unable to close listener: %s", ts.rf.me, err.Error())
	}
	ts.wg.Wait() // wait for server to stop serving
}

type TestCluster struct {
	cluster []*TestServer
	t *testing.T
}

func NewTestCluster(n int, t *testing.T) *TestCluster {
	servers := make([]*TestServer, n)

	for i := 0; i < n; i++ {
		peers := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peers = append(peers, p)
			}
		}
		servers[i] = NewServer(i, peers, t)
		servers[i].Start()
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				servers[j].rf.mu.Lock()
				addr := servers[j].rf.listener.Addr()
				servers[j].rf.mu.Unlock()
				servers[i].rf.mu.Lock()
				servers[i].Connect(j, addr)
				servers[i].rf.mu.Unlock()
				log.Printf("[c] %d connected to %d", i, j)
			}
		}
	}

	tc := &TestCluster{cluster: servers, t:t}
	return tc
}

// KillCluster kills the connection between each Raft server
func (tc *TestCluster) KillCluster() {
	log.Printf("[c] closing connections amongst Raft servers and killing Raft servers")
	for _, ts := range tc.cluster {
		ts.DisconnectPeers()
	}
	for _, ts := range tc.cluster {
		ts.Kill()
	}
}

// DisconnectServer disconnects a Raft server from its peers
func (tc *TestCluster) DisconnectServer(id int) {

	log.Printf("[c] disconnecting %d to rest of the cluster", id)
	tc.cluster[id].DisconnectPeers() // disconnect id from its peers
	for i := 0; i < len(tc.cluster); i++ {
		if i == id {
			continue
		}
		tc.cluster[i].rf.mu.Lock()
		tc.cluster[i].Disconnect(id) // disconnect peer from id
		tc.cluster[i].rf.mu.Unlock()
	}
}

func (tc *TestCluster) DisconnectCluster() {
	for id, _ := range tc.cluster {
		tc.DisconnectServer(id)
	}
}

// ConnectServer connects a Raft server to all of its peers
func (tc *TestCluster) ConnectServer(id int) {

	log.Printf("[c] connecting %d to rest of the cluster", id)
	for i := 0; i < len(tc.cluster); i++ {
		if i != id {
			tc.cluster[i].rf.mu.Lock()
			tc.cluster[id].rf.mu.Lock()
			tc.cluster[id].Connect(i, tc.cluster[i].rf.listener.Addr())
			tc.cluster[i].Connect(id, tc.cluster[id].rf.listener.Addr())
			tc.cluster[i].rf.mu.Unlock()
			tc.cluster[id].rf.mu.Unlock()
		}
	}
}

func (tc *TestCluster) ConnectCluster() {
	for id, _ := range tc.cluster {
		tc.ConnectServer(id)
	}
}

func connectedToPeers(ts *TestServer) bool {
	for _, client := range ts.rf.clients {
		if client != nil {
			return true
		}
	}
	return false
}




// FindLeader looks for and returns the current leader
// If no leader or more leaders are found, the test will fail
func (tc *TestCluster) FindLeader(t *testing.T) (int, int) {
	for r:=0; r<5; r++ {
		leaderId, leaderTerm := -1, -1
		for i := 0; i < len(tc.cluster); i++ {
			ts := tc.cluster[i]
			// it's ok for a disconnected server to think it is leader
			// as no logs are appended without quorum.
			// when rejoining its peers, it will switch to follower
			ts.rf.mu.Lock()
			state := ts.rf.state
			ts.rf.mu.Unlock()
			if connectedToPeers(ts) && state == Leader {
				if leaderId == -1 {
					leaderId = i
					ts.rf.mu.Lock()
					leaderTerm = ts.rf.currentTerm
					ts.rf.mu.Unlock()
				} else {
					t.Errorf("%d and %d are both leaders", leaderId, i)
				}
			}

			// leader found, return ...
			if leaderId >= 0 {
				return leaderId, leaderTerm
			}
			// ... otherwise, wait for things to settle and try again
			time.Sleep(ServerSlack)
		}
	}

	t.Errorf("no leader elected")
	return -1, -1
}

func NoQuorum(tc *TestCluster, t *testing.T) {
	for _, ts := range tc.cluster {
		if connectedToPeers(ts) && ts.rf.state == Leader {
			t.Errorf("%d became a leader without quorum", ts.rf.me)
		}
	}
}

func connectedToPeers(ts *TestServer) bool {
	for _, client := range ts.rf.clients {
		if client != nil {
			return true
		}
	}
	return false
}