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

// DoneMsg is sent back to Raft client after a requested command is committed
type DoneMsg struct {
	Index   int // log index
	Term    int
	Command interface{}
}

type RPCServer struct {
	server   *rpc.Server
	clients  map[int]*rpc.Client
	listener net.Listener
}
