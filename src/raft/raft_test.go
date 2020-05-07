package raft

import (
	"testing"
	"time"
)

//
//func TestElection(t *testing.T) {
//	tc := NewTestCluster(3, t)
//	defer tc.DisconnectCluster()
//
//	FindLeader(tc, t)
//}

func TestLeaderDown(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.DisconnectCluster()

	leaderId, term := FindLeader(tc, t)

	tc.cluster[leaderId].DisconnectPeers()

	// after heartbeat interval, followers will realise leader is down
	time.Sleep(200*time.Millisecond)

	newLeaderId, newTerm := FindLeader(tc, t)
	if newLeaderId == leaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= term {
		t.Errorf("want newTerm <= term, got %d and %d", newTerm, term)
	}
}