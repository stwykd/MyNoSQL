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
	wg       sync.WaitGroup
}

func NewServer(id int, peers []int) *TestServer {
	s := new(TestServer)
	s.rf = NewRaft(id, peers)
	return s
}

func (s *TestServer) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.rf.listener.Accept()
			if err != nil {
				log.Fatal("accept error:", err)
			}
			s.wg.Add(1)
			go func() {
				s.rf.server.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *TestServer) Connect(id int, addr net.Addr) {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()
	if s.rf.clients[id] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			log.Fatalf("unable to connect peers: %s", err.Error())
		}
		s.rf.clients[id] = client
	}
}

type TestCluster struct {
	cluster []*TestServer
	t *testing.T
}

func NewTestCluster(t *testing.T, n int) *TestCluster {
	servers := make([]*TestServer, n)

	for i := 0; i < n; i++ {
		peers := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peers = append(peers, p)
			}
		}

		servers[i] = NewServer(i, peers)
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


// FindLeader looks for and returns the current leader
// If no leader or more leaders are found, the test will fail
func FindLeader(tc *TestCluster, t *testing.T) (int, int) {
	for r := 0; r < 5; r++ {
		leaderId, leaderTerm := -1, -1
		for i := 0; i < len(tc.cluster); i++ {
			rf := tc.cluster[i].rf
			term, isLeader := rf.currentTerm, rf.state == Leader
			if isLeader {
				if leaderId < 0 {
					leaderId = i
					leaderTerm = term
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