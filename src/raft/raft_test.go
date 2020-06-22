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

	_, newTerm := tc.FindLeader(t)
	if newTerm <= term {
		t.Errorf("newTerm <= term after reconnecting follower")
	}
}




// === LOG REPLICATION ===

func TestReplicate(t *testing.T) {
	cmd, n := 1, 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()
	time.Sleep(ClusterSlack)
	leader, _ := tc.FindLeader(t)

	isLeader := Replicate(tc.cluster[leader].rf, cmd)
	if !isLeader {
		t.Errorf("expected leader id to be %d", leader)
	}

	time.Sleep(ServerSlack)
	if nGot, _ := tc.Committed(cmd); nGot != n {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmd, n)
	}
}

func TestReplicateMore(t *testing.T) {
	cmds, n := []int{0, 1, 2}, 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader(t)

	for _, cmd := range cmds {
		if !Replicate(tc.cluster[leader].rf, cmd) {
			t.Errorf("expected %d to be leader", leader)
		}
		time.Sleep(ServerSlack)
	}

	nServers0, idx0 := tc.Committed(cmds[0])
	nServers1, idx1 := tc.Committed(cmds[1])
	nServers2, idx2 := tc.Committed(cmds[2])
	if nServers0 != n || nServers1 != n || nServers2 != n {
		t.Errorf("one or more commands wasn't replicated by all servers")
	}
	if idx0 >= idx1 {
		t.Errorf("cmd %d was replicated before cmd %d, but cmd %d has lower idx: idx0:%d idx1:%d",
			cmds[0], cmds[1], cmds[0], idx0, idx1)
	}
	if idx1 >= idx2 {
		t.Errorf("cmd %d was replicated before cmd %d, but cmd %d has lower idx: idx0:%d idx1:%d",
			cmds[0], cmds[2], cmds[0], idx0, idx1)
	}
}

func TestReplicateFollower(t *testing.T) {
	cmd, n := 1, 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader(t)
	other := (leader + 1) % n
	if Replicate(tc.cluster[other].rf, cmd) { // can only call Replicate on a leader
		t.Errorf("replicated %d to non-leader %d", cmd, other)
	}
}

func TestReplicateReconnect(t *testing.T) {
	cmds, n := []int{0, 1, 2}, 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader(t)
	Replicate(tc.cluster[leader].rf, cmds[0])
	Replicate(tc.cluster[leader].rf, cmds[1])

	time.Sleep(ServerSlack)
	if nGot, _ := tc.Committed(cmds[1]); nGot != n {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[1], n)
	}

	other := (leader + 1) % 3
	tc.DisconnectServer(other)
	time.Sleep(ServerSlack)

	Replicate(tc.cluster[leader].rf, cmds[2])
	time.Sleep(ClusterSlack)
	if nGot, _ := tc.Committed(cmds[2]); nGot != n-1 {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[2], n-1)
	}

	tc.ConnectServer(other)
	time.Sleep(ClusterSlack)
	tc.FindLeader(t)
	time.Sleep(ServerSlack)
	if nGot, _ := tc.Committed(cmds[2]); nGot != n {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[2], n)
	}
}

func TestReplicateLeaderReconnect(t *testing.T) {
	cmds, n := []int{0, 1, 2, 3, 4}, 5
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader(t)
	Replicate(tc.cluster[leader].rf, cmds[0])
	Replicate(tc.cluster[leader].rf, cmds[1])

	time.Sleep(ServerSlack)
	if nGot, _ := tc.Committed(cmds[1]); nGot != n {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[1], n)
	}

	tc.DisconnectServer(leader)

	Replicate(tc.cluster[leader].rf, cmds[2])
	time.Sleep(ServerSlack)
	tc.NotCommitted(cmds[2])

	newLeader, _ := tc.FindLeader(t)

	Replicate(tc.cluster[newLeader].rf, cmds[3])
	time.Sleep(ServerSlack)
	if nGot, _ := tc.Committed(cmds[3]); nGot != n-1 {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[3], n-1)
	}

	tc.ConnectServer(leader)
	time.Sleep(ClusterSlack)

	newNewLeader, _ := tc.FindLeader(t)
	if newNewLeader == leader {
		t.Errorf("leader remained %d", leader)
	}

	Replicate(tc.cluster[newLeader].rf, cmds[4])
	time.Sleep(ServerSlack)
	if nGot, _ := tc.Committed(cmds[4]); nGot != n {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[4], n)
	}
	if nGot, _ := tc.Committed(cmds[3]); nGot != n {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[3], n)
	}
	tc.NotCommitted(cmds[2])
}


func TestReplicateNoMajority(t *testing.T) {
	cmds, n := []int{0, 1, 2, 3, 4, 5}, 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, term := tc.FindLeader(t)
	Replicate(tc.cluster[leader].rf, cmds[0])
	Replicate(tc.cluster[leader].rf, cmds[1])

	time.Sleep(ServerSlack*2)
	if nGot, _ := tc.Committed(cmds[1]); nGot != n {
		tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[1], n)
	}

	other1 := (leader + 1) % n
	other2 := (other1 + 1) % n
	tc.DisconnectServer(other1)
	tc.DisconnectServer(other2)
	time.Sleep(ServerSlack*2)

	Replicate(tc.cluster[leader].rf, cmds[2])
	time.Sleep(ServerSlack)
	tc.NotCommitted(cmds[2])

	tc.ConnectServer(other1)
	tc.ConnectServer(other2)
	time.Sleep(ClusterSlack)

	tc.NotCommitted(cmds[2])

	newLeader, newTerm := tc.FindLeader(t)
	if term == newTerm {
		t.Errorf("term remained %d", term)
	}

	Replicate(tc.cluster[newLeader].rf, cmds[3])
	Replicate(tc.cluster[newLeader].rf, cmds[4])
	Replicate(tc.cluster[newLeader].rf, cmds[5])
	time.Sleep(ClusterSlack)

	for _, cmd := range cmds[3:] {
		if nGot, _ := tc.Committed(cmd); nGot != n {
			tc.t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmd, n)
		}
	}
}