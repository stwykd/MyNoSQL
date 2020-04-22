package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

const BasePort = 30000

func getPeers(cluster map[int]*rpc.Client, me, nServers int) map[int]*rpc.Client {
	peers := make(map[int]*rpc.Client, nServers-1)
	for i, p := range cluster {
		if i != me {
			peers[i] = p
		}
	}
	return peers
}

func listener(port int) net.Listener {
	l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if e != nil {
		log.Fatalf("net.Listen tcp :%d: %v", port, e)
	}
	return l
}

func setupRaftCluster(raftServers []*Raft, nServers int) {
	raftServers = make([]*Raft, nServers)
	servers := make(map[int]*rpc.Server, nServers)
	clients := make(map[int]map[int]*rpc.Client, nServers)

	for i:=0; i<nServers; i++ {
		clients[i] = make(map[int]*rpc.Client)
	}

	wg := sync.WaitGroup{}

	for i :=0; i<nServers; i++ {
		servers[i] = rpc.NewServer()
		if err := servers[i].Register(new(Raft)); err != nil {
			log.Fatalf("unable to register Raft in server %d: %s", i, err.Error())
		}
		log.Printf("[%v] registered Raft server", i)

		for j:=0; j<nServers; j++ {
			if i == j {
				continue
			}

			port := BasePort+(i*10)+j
			l := listener(port)
			addr := l.Addr().String()
			wg.Add(1)
			go func(i, j int) {
				wg.Wait()
				log.Printf("[%v] dialing %s ...", j, addr)
				c, err := net.Dial("tcp", addr)
				if err != nil {
					log.Fatalf("[%v] unable to dial server at %s: %s", j, addr, err.Error())
				}
				clients[j][i] = rpc.NewClient(c)
				log.Printf("[%v] connected to %s", j, addr)
			}(i, j)

			log.Printf("[%v] listening for connections at %s", i, addr)
			wg.Done()
			conn, err := l.Accept()
			if err != nil {
				log.Fatalf("[%v] error while accepting connections: %s", i, err.Error())
			}
			log.Printf("[%v] connected to %s", i, addr)
			go func(i int) {
				log.Printf("[%v] starting to serve %s", i, addr)
				servers[i].ServeConn(conn)
			}(i)

		}
	}

	for i:=0; i<nServers; i++ {
		raftServers[i] = NewRaft(i, getPeers(clients[i], i, nServers), servers[i])
	}
}
