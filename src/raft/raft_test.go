package raft

import (
	"testing"
	"time"
)


// === LEADER ELECTION ===

func TestElection(t *testing.T) {
	// setup new test cluster with 3 machines
	tc := NewTestCluster(3, t)
	// prevent goroutines from affecting other tests
	defer tc.KillCluster()

	tc.FindLeader(t)
}

func TestLeaderDown(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	leader, term := tc.FindLeader(t)

	tc.cluster[leader].DisconnectPeers()

	time.Sleep(ServerSlack)

	newLeader, newTerm := tc.FindLeader(t)
	if newLeader == leader {
		t.Errorf("leader didn't change after disconencting leader")
	}
	if newTerm <= term {
		t.Errorf("term didn't change after disconnecting leader")
	}
}

func TestReconnectLeader(t *testing.T) {
	tc := NewTestCluster(5, t)
	defer tc.KillCluster()
	leader, _ := tc.FindLeader(t)

	tc.DisconnectServer(leader)
	time.Sleep(ServerSlack)
	newLeader, newTerm := tc.FindLeader(t)
	tc.ConnectServer(leader)
	time.Sleep(ServerSlack)

	newLeader_, newTerm_ := tc.FindLeader(t)

	if newLeader != newLeader_ {
		t.Errorf("leader changed from %d to %d", newLeader, newLeader_)
	}
	if newTerm_ != newTerm {
		t.Errorf("term changed from %d to %d", newTerm, newTerm_)
	}
}

func TestElectionNoMajority(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader(t)

	// remove 2/3 servers
	server := (leader+1)%len(tc.cluster)
	tc.DisconnectServer(leader)
	tc.DisconnectServer(server)

	time.Sleep(ServerSlack*2)

	NoQuorum(tc, t)
	tc.ConnectServer(server)
	tc.FindLeader(t)
}

func TestRestartCluster(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	tc.DisconnectCluster()
	time.Sleep(ClusterSlack)
	NoQuorum(tc, t)

	tc.ConnectCluster()
	tc.FindLeader(t)
}

func TestKeepResettingLeader(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	for i := 0; i < 5; i++ {
		leaderId, _ := tc.FindLeader(t)

		tc.DisconnectServer(leaderId)
		otherId := (leaderId + 1) % 3
		tc.DisconnectServer(otherId)
		time.Sleep(ServerSlack)
		NoQuorum(tc, t)

		tc.ConnectServer(otherId)
		tc.ConnectServer(leaderId)

		time.Sleep(ServerSlack)
	}
}

func TestReconnectFollower(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	leader, term := tc.FindLeader(t)

	other := (leader + 1) % 3
	tc.DisconnectServer(other)
	time.Sleep(ClusterSlack)
	tc.ConnectServer(other)
	time.Sleep(ServerSlack)

	_, newTerm := FindLeader(tc, t)
	if newTerm <= term {
		t.Errorf("newTerm <= term after reconnecting follower")
	}
}