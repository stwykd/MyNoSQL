package raft

import (
	"fmt"
	//"github.com/golang/mock/gomock"
	"log"
	"net"
	"net/rpc"
	"testing"
)

const ClusterSize = 5

var raftServers []*Raft

func TestMain(m *testing.M) {
	setupRaftCluster(ClusterSize)
	m.Run()
}

func getPeers(cluster map[int]*rpc.Client, me int) map[int]*rpc.Client {
	peers := make(map[int]*rpc.Client, ClusterSize-1)
	for i, p := range cluster {
		if i != me {
			peers[i] = p
		}
	}
	return peers
}

func listenTCP(port int) (net.Listener, string) {
	l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if e != nil {
		log.Fatalf("net.Listen tcp :%d: %v", port, e)
	}
	return l, l.Addr().String()
}

func setupRaftCluster(nServers int) {
	raftServers = make([]*Raft, ClusterSize)
	servers := make(map[int]*rpc.Server, ClusterSize)
	clients := make(map[int]*rpc.Client, ClusterSize)

	for i:=0; i<nServers; i++ {
		servers[i] = rpc.NewServer()
		if err := servers[i].Register(new(Raft)); err != nil {
			log.Fatalf("unable to register Raft in server %d: %s", i, err.Error())
		}

		l, addr := listenTCP(30000+i)
		fmt.Printf(addr, " ", l.Addr().String())
		go rpc.Accept(l)
		c, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatalf("unable to dial server at %s: %s", addr, err.Error())
		}
		clients[i] = rpc.NewClient(c)
	}

	for i:=0; i<nServers; i++ {
		raftServers[i] = NewRaft(i, getPeers(clients, i), servers[i])
	}
}
