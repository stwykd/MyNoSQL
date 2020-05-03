package raft

import (
	"testing"
)


func TestElection(t *testing.T) {
	tc := NewTestCluster(t, 3)
	FindLeader(tc, t)
}