package raft

import (
	"net/rpc"
	"time"
)

const HeartbeatInterval = 50 * time.Millisecond

// LogEntry is a single entry in a Raft server's log
type LogEntry struct {
	Index int
	Term  int
	Command interface{}
}

// State of a Raft server
type State int
const (
	Leader State = iota
	Follower
	Candidate
)

// DoneMsg is sent back to Raft client after a requested command is committed
type DoneMsg struct {
	Index int // log index
	Term  int
	Command interface{}
}

// Cluster is a map from server id to RPC endpoint
type Cluster map[int]*rpc.Client