package src

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

// Server serves and sends RPC requests for a Raft instance
type Server struct {
	id       int
	rf       *Raft
	storage  Storage
	rfServer *rpc.Server // listens for Raft RPC requests from peers
	listener net.Listener
	commitCh chan<- Commit
	peers    map[int]*rpc.Client
	ready    <-chan interface{}
	quit     chan interface{}
	wg       sync.WaitGroup
	server   *rpc.Server // listens for Server RPC requests from client app
	data     map[string]string // store Key-Value pair
}

// NewServer instantiates a new Server. the rfServer can be then run using Run()
func NewServer(id int, peers []int, storage Storage, ready <-chan interface{}, commitCh chan<- Commit) *Server {
	s := new(Server)
	s.id = id
	s.peers = make(map[int]*rpc.Client)
	s.storage = storage
	s.ready = ready
	s.commitCh = commitCh
	s.quit = make(chan interface{})
	s.rf = NewRaft(s.id, peers, s, s.storage, s.ready, s.commitCh)
	s.data = make(map[string]string)
	return s
}

// Run starts a new RPC rfServer, which begins listening for requests
// s.wg can used to halt execution and start all servers simultaneously
func (s *Server) Run() {
	s.rf.mu.Lock()

	s.rfServer = rpc.NewServer()
	if err := s.rfServer.RegisterName("Raft", s.rf); err != nil {
		log.Fatal(err)
	}

	s.server = rpc.NewServer()
	if err := s.rfServer.RegisterName("Server", s); err != nil {
		log.Fatal(err)
	}

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.id, s.listener.Addr())
	s.rf.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rfServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

// GetListenAddr returns listener address for this rfServer
func (s *Server) GetListenAddr() net.Addr {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()
	return s.listener.Addr()
}

// Connect connects test rfServer to RPC client with id `id`
// requires mu to be acquired
func (s *Server) Connect(peer int, addr net.Addr) error {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()
	if s.peers[peer] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peers[peer] = client
	}
	return nil
}

// Disconnect disconnects RPC client with id `id` from test rfServer
// requires mu to be acquired
func (s *Server) Disconnect(peer int) error {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()
	if s.peers[peer] != nil {
		err := s.peers[peer].Close()
		s.peers[peer] = nil
		return err
	}
	return nil
}

func (s *Server) DisconnectPeers() {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()
	for id := range s.peers {
		if s.peers[id] != nil {
			if err := s.peers[id].Close(); err != nil {
				log.Fatal(err)
			}
			s.peers[id] = nil
		}
	}
}

func (s *Server) Kill() {
	s.rf.Stop()
	close(s.quit)
	if err := s.listener.Close(); err != nil {
		log.Fatal(err)
	}
	s.wg.Wait()
}

// Call makes an RPC call to Raft peer identified by id
func (s *Server) Call(id int, method string, args interface{}, reply interface{}) error {
	s.rf.mu.Lock()
	peer := s.peers[id]
	s.rf.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("client %d closed", id)
	} else {
		return peer.Call(method, args, reply)
	}
}



func (s *Server) Get(args GetArgs, reply *GetReply) error {
	if leader := s.rf.Replicate(args.Key); !leader {
		reply.Success=false
		return nil
	}

	if s.rf.leader != s.rf.me { // only leader should receives client requests
		if err := s.Call(s.rf.leader, "Server.Get", args, reply); err != nil {
			return err
		}
	} else {
		reply.Value=s.data[args.Key]
		reply.Success=true
	}
	return nil
}

// TODO Replay log when leader changes to have consistent s.data after crash
func (s *Server) Put(args PutArgs, reply *PutReply) error {
	log.Printf("[%d] received Put RPC: %s->%s", s.rf.me, args.Key, args.Value)
	if leader := s.rf.Replicate(args.Key+" "+args.Value); !leader { // gob.Encode(args)
		reply.Success = false
		return nil
	}

	s.data[args.Key] = args.Value
	reply.Success=true

	return nil
}









// Cluster simulates a rfServer cluster. it is used exclusively for testing
type Cluster struct {
	mu        sync.Mutex
	cluster   []*Server
	servers   []*rpc.Server
	storage   []Storage
	commitChs []chan Commit
	commits   [][]Commit
	connected []bool
	alive     []bool
	n         int
}

// NewCluster creates a new Cluster, initialized with n servers connected
// to each other
func NewCluster(storages []Storage, n int) *Cluster {
	servers := make([]*Server, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	clientChs := make([]chan Commit, n)
	commits := make([][]Commit, n)
	ready := make(chan interface{})
	storage := make([]Storage, n)

	for i := 0; i < n; i++ {
		peers := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peers = append(peers, p)
			}
		}

		storage[i] = storages[i]
		clientChs[i] = make(chan Commit)
		servers[i] = NewServer(i, peers, storage[i], ready, clientChs[i])
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

	tc := &Cluster{
		cluster:   servers,
		storage:   storage,
		commitChs: clientChs,
		commits:   commits,
		connected: connected,
		alive:     alive,
		n:         n,
	}
	for i := 0; i < n; i++ {
		go tc.listenCommits(i)
	}
	return tc
}

// KillCluster shuts down all the servers in the harness and waits for them to
// stop running
func (tc *Cluster) KillCluster() {
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
		close(tc.commitChs[id])
	}
}

// Disconnect disconnects a rfServer from all other servers in the cluster
func (tc *Cluster) Disconnect(id int) {
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

// Connect connects a rfServer to all other servers in the cluster
func (tc *Cluster) Connect(id int) {
	for j := 0; j < tc.n; j++ {
		if j != id && tc.alive[j] {
			if err := tc.cluster[id].Connect(j, tc.cluster[j].GetListenAddr()); err != nil {
				log.Fatal(err)
			}
			if err := tc.cluster[j].Connect(id, tc.cluster[id].GetListenAddr()); err != nil {
				log.Fatal(err)
			}
		}
	}
	tc.connected[id] = true
}

func (tc *Cluster) listenCommits(id int) {
	log.Printf("[%v] start listening for commits", tc.cluster[id].rf.me)
	for c := range tc.commitChs[id] {
		tc.mu.Lock()
		tc.commits[id] = append(tc.commits[id], c)
		tc.mu.Unlock()
	}
}

func (tc *Cluster) connectToServers() []*rpc.Client {
	servers := make([]*rpc.Client, len(tc.servers))
	for _, server := range tc.cluster {
		go server.server.Accept(server.listener)
	}
	for _, server := range tc.cluster {
		addr := server.GetListenAddr()
		if client, err := rpc.Dial(addr.Network(), addr.String()); err != nil {
			log.Fatalf("error while dialing server: %s", err.Error())
		} else {
			servers=append(servers, client)
		}
	}

	return servers
}