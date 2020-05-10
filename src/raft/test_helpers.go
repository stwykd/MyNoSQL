package raft

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"
)

type TestServer struct {
	rf       *Raft
	t	     *testing.T
	wg       sync.WaitGroup
	close	 chan interface{}
}

func NewServer(id int, peers []int, t *testing.T) *TestServer {
	s := new(TestServer)
	s.t = t
	s.rf = NewRaft(id, peers)
	s.close = make(chan interface{})
	return s
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
					ts.t.Fatalf("accept error: %s", err)
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

func (ts *TestServer) Connect(id int, addr net.Addr) {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	if ts.rf.clients[id] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			ts.t.Fatalf("%d couldn't dial %d: %s", ts.rf.me, id, err.Error())
		}
		ts.rf.clients[id] = client
	}
}

// Disconnect disconnects RPC client with id `id`
func (ts *TestServer) Disconnect(id int) {
	ts.rf.mu.Lock()
	defer ts.rf.mu.Unlock()
	if ts.rf.clients[id] != nil {
		if err := ts.rf.clients[id].Close(); err != nil {
			ts.t.Fatalf("[%v] unable to close connection to %d: %s", ts.rf.me, id, err.Error())
		}
		ts.rf.clients[id] = nil
	}
}

// DisconnectPeers disconnects all RPC clients
func (ts *TestServer) DisconnectPeers() {
	for id := range ts.rf.clients {
		ts.Disconnect(id)
	}
}

// Close closes the RPC server
func (ts *TestServer) Close() {
	ts.rf.Kill()
	close(ts.close)
	if err := ts.rf.listener.Close(); err != nil {
		ts.t.Fatalf("[%v] unable to close listener: %s", ts.rf.me, err.Error())
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
				servers[i].Connect(j, addr)
			}
		}
	}

	return &TestCluster{cluster: servers, t: t}
}

// KillCluster kills the connection between each Raft server
func (tc *TestCluster) KillCluster() {
	log.Printf("[c] closing connectings amongst Raft server")
	for _, ts := range tc.cluster {
		ts.DisconnectPeers()
	}
	for _, ts := range tc.cluster {
		ts.Close()
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
		tc.cluster[i].Disconnect(id) // disconnect peer from id
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
	for j := 0; j < len(tc.cluster); j++ {
		if j != id {
			tc.cluster[id].Connect(j, tc.cluster[j].rf.listener.Addr())
			tc.cluster[j].Connect(id, tc.cluster[id].rf.listener.Addr())
		}
	}
}

func (tc *TestCluster) ConnectCluster() {
	for id, _ := range tc.cluster {
		tc.ConnectServer(id)
	}
}

// ==== TESTING METHODS ====

// FindLeader looks for and returns the current leader
// If no leader or more leaders are found, the test will fail
func FindLeader(tc *TestCluster, t *testing.T) (int, int) {
	for r := 0; r < 5; r++ {
		leaderId, leaderTerm := -1, -1
		for i := 0; i < len(tc.cluster); i++ {
			rf := tc.cluster[i].rf
			if rf.state == Leader {
				if leaderId  == -1 {
					leaderId = i
					leaderTerm = rf.currentTerm
				} else {
					t.Fatalf("%d and %d are both leaders", leaderId, i)
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(minElectionWait * time.Millisecond)
	}

	t.Fatalf("no leader elected")
	return -1, -1
}

func NoLeader(tc *TestCluster, t *testing.T) {
	for _, ts := range tc.cluster {
		if ConnectedToPeers(ts) && ts.rf.state == Leader {
			t.Fatalf("%d became a leader without quorum", ts.rf.me)
		}
	}
}

func ConnectedToPeers(ts *TestServer) bool {
	for _, client := range ts.rf.clients {
		if client != nil {
			return true
		}
	}
	return false
}