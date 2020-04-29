package raft

import (
	"log"
	"net"
	"net/rpc"
	"sync"
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

