package raft

import (
	"math/rand"
	"time"
)


const minElectionTimer, maxElectionTimer = 150, 300

// generates a random duration between 150ms and 300 ms
func randElectionTimer() *time.Timer {
	return time.NewTimer(time.Duration(rand.Intn(maxElectionTimer)-minElectionTimer))
}