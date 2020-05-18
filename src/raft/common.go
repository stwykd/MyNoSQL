package raft

import (
	"net"
	"net/rpc"
	"time"
)

const HeartbeatInterval = 50 * time.Millisecond

// LogEntry is a single entry in a Raft server's log
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// State of a Raft server
type State string

const (
	Leader    State = "leader"
	Follower        = "follower"
	Candidate       = "candidate"
	Down            = "down" // for testing
)

// CommitMsg is used to commitCh the client that a command was replicated by
// a majority (ie it was committed) and it can now be applied by client
type CommitMsg struct {
	Index   int // log index
	Term    int
	Command interface{}
}

type RPCServer struct {
	server   *rpc.Server
	clients  map[int]*rpc.Client
	listener net.Listener
}
