package raft

import (
	"testing"
	"time"
)


func TestElection(t *testing.T) {
	tc := NewTestCluster(3, t)
	// prevents goroutines still running from affecting other tests
	defer tc.KillCluster()

	FindLeader(tc, t)
}

func TestLeaderDown(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	leader, term := FindLeader(tc, t)

	tc.cluster[leader].DisconnectPeers()

	time.Sleep(ServerSlack)

	newLeader, newTerm := FindLeader(tc, t)
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
	leader, _ := FindLeader(tc, t)

	tc.DisconnectServer(leader)
	time.Sleep(ServerSlack)
	newLeader, newTerm := FindLeader(tc, t)
	tc.ConnectServer(leader)
	time.Sleep(ServerSlack)

	newLeader_, newTerm_ := FindLeader(tc, t)

	if newLeader != newLeader_ {
		t.Errorf("leader changed from %d to %d", newLeader, newLeader_)
	}
	if newTerm_ != newTerm {
		t.Errorf("term changed from %d to %d", newTerm, newTerm_)
	}
}

func TestNoQuorum(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	leader, _ := FindLeader(tc, t)

	// remove 2/3 servers
	server := (leader+1)%len(tc.cluster)
	tc.DisconnectServer(leader)
	tc.DisconnectServer(server)

	time.Sleep(ServerSlack*2)

	NoQuorum(tc, t)
	tc.ConnectServer(server)
	FindLeader(tc, t)
}

func TestRestartCluster(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	tc.DisconnectCluster()
	time.Sleep(ClusterSlack)
	NoQuorum(tc, t)

	tc.ConnectCluster()
	FindLeader(tc, t)
}

