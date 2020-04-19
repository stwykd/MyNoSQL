package raft

import (
	"fmt"
	//"github.com/golang/mock/gomock"
	"log"
	"net"
	"net/rpc"
)

const ClusterSize = 5

var raftServers []*Raft

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
