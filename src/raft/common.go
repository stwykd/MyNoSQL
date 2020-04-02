package raft

// LogEntry is a single entry in a Raft server's log
type LogEntry struct {
	Index int
	Term  int
	Command interface{}
}

// Role of a Raft server
type Role int
const (
	Leader Role = iota
	Follower
	Candidate
)

// DoneMsg is sent back to Raft client after a requested command is committed
type DoneMsg struct {
	Index int // log index
	Term  int
	Command interface{}
}