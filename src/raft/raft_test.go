package raft

import (
	"testing"
	"time"
)


func TestElection(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	FindLeader(tc, t)
}

func TestLeaderDown(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	leaderId, term := FindLeader(tc, t)

	tc.cluster[leaderId].DisconnectPeers()

	time.Sleep(200*time.Millisecond)

	newLeaderId, newTerm := FindLeader(tc, t)
	if newLeaderId == leaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= term {
		t.Errorf("want newTerm <= term, got %d and %d", newTerm, term)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	leader, _ := FindLeader(tc, t)

	// remove 2/3 servers
	server := (leader+1)%len(tc.cluster)
	tc.DisconnectServer(leader)
	tc.DisconnectServer(server)

	time.Sleep(350*time.Millisecond)

	NoLeader(tc, t)
	tc.ConnectServer(server)
	FindLeader(tc, t)
}

func TestDisconnectAllThenRestore(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	time.Sleep(100*time.Millisecond)
	tc.DisconnectCluster()
	time.Sleep(450*time.Millisecond)
	NoLeader(tc, t)

	// Reconnect all servers. A leader will be found.
	tc.ConnectCluster()
	FindLeader(tc, t)
}