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

	time.Sleep(250*time.Millisecond)

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

	tc.Disconnect(leader)
	time.Sleep(250*time.Millisecond)
	newLeader, newTerm := tc.FindLeader(t)
	tc.Connect(leader)
	time.Sleep(250*time.Millisecond)

	newLeader_, newTerm_ := tc.FindLeader(t)

	if newLeader != newLeader_ {
		t.Errorf("leader changed from %d to %d", newLeader, newLeader_)
	}
	if newTerm_ != newTerm {
		t.Errorf("term changed from %d to %d", newTerm, newTerm_)
	}
}

func TestElectionNoMajority(t *testing.T) {
	n := 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader(t)

	// remove 2/3 servers
	server := (leader+1)%n
	tc.Disconnect(leader)
	tc.Disconnect(server)

	time.Sleep(450*time.Millisecond)

	NoQuorum(tc, t)
	tc.Connect(server)
	tc.FindLeader(t)
}

func TestRestartCluster(t *testing.T) {
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	tc.KillNetwork()
	time.Sleep(450*time.Millisecond)
	NoQuorum(tc, t)

	tc.StartNetwork()
	tc.FindLeader(t)
}

func TestKeepResettingLeader(t *testing.T) {
	n := 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	for resets := 0; resets < 5; resets++ {
		leaderId, _ := tc.FindLeader(t)

		tc.Disconnect(leaderId)
		otherId := (leaderId + 1) % n
		tc.Disconnect(otherId)
		time.Sleep(250*time.Millisecond)
		NoQuorum(tc, t)

		tc.Connect(otherId)
		tc.Connect(leaderId)

		time.Sleep(250*time.Millisecond)
	}
}

func TestReconnectFollower(t *testing.T) {
	n := 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, term := tc.FindLeader(t)

	other := (leader + 1) % n
	tc.Disconnect(other)
	time.Sleep(450*time.Millisecond)
	tc.Connect(other)
	time.Sleep(250*time.Millisecond)

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
	time.Sleep(450*time.Millisecond)
	leader, _ := tc.FindLeader(t)

	if !Replicate(tc.cluster[leader].rf, cmd) {
		t.Errorf("expected %d to be leader", leader)
	}

	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmd); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmd, n)
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
		time.Sleep(250*time.Millisecond)
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
	if !Replicate(tc.cluster[leader].rf, cmds[0]) {
		t.Errorf("expected %d to be leader", leader)
	}
	if !Replicate(tc.cluster[leader].rf, cmds[1]) {
		t.Errorf("expected %d to be leader", leader)
	}

	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[1]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[1], n)
	}

	other := (leader + 1) % n
	tc.Disconnect(other)
	time.Sleep(250*time.Millisecond)
	if !Replicate(tc.cluster[leader].rf, cmds[2]) {
		t.Errorf("expected %d to be leader", leader)
	}
	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[2]); nGot != n-1 {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[2], n-1)
	}

	tc.Connect(other)
	time.Sleep(250*time.Millisecond)
	tc.FindLeader(t)
	time.Sleep(150*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[2]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[2], n)
	}
}

func TestReplicateLeaderReconnect(t *testing.T) {
	cmds, n := []int{0, 1, 2}, 3
	tc := NewTestCluster(3, t)
	defer tc.KillCluster()

	// Submit a couple of values to a fully connected cluster.
	leader, _ := tc.FindLeader(t)
	if !Replicate(tc.cluster[leader].rf, cmds[0]) {
		t.Errorf("expected %d to be leader", leader)
	}
	if !Replicate(tc.cluster[leader].rf, cmds[1]) {
		t.Errorf("expected %d to be leader", leader)
	}
	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[1]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[1], n)
	}

	// Disconnect leader for a short time (less than election timeout in peers).
	tc.Disconnect(leader)
	time.Sleep(90*time.Millisecond)
	tc.Connect(leader)
	time.Sleep(200*time.Millisecond)

	if !Replicate(tc.cluster[leader].rf, cmds[2]) {
		t.Errorf("expected %d to be leader", leader)
	}
	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[2]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[2], n)
	}
}

func TestReplicateLeaderReconnectMore(t *testing.T) {
	cmds, n := []int{0, 1, 2, 3, 4}, 5
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader(t)
	if !Replicate(tc.cluster[leader].rf, cmds[0]) {
		t.Errorf("expected %d to be leader", leader)
	}
	if !Replicate(tc.cluster[leader].rf, cmds[1]) {
		t.Errorf("expected %d to be leader", leader)
	}
	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[1]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[1], n)
	}

	tc.Disconnect(leader)
	time.Sleep(10*time.Millisecond)

	if !Replicate(tc.cluster[leader].rf, cmds[2]) {
		t.Errorf("expected %d to be leader", leader)
	}
	time.Sleep(250*time.Millisecond)
	tc.NotCommitted(cmds[2])

	newLeader, _ := tc.FindLeader(t)

	if !Replicate(tc.cluster[newLeader].rf, cmds[3]) {
		t.Errorf("expected %d to be leader", leader)
	}
	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[3]); nGot != n-1 {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[3], n-1)
	}

	tc.Connect(leader)
	time.Sleep(600*time.Millisecond)

	newNewLeader, _ := tc.FindLeader(t)
	if newNewLeader == leader {
		t.Errorf("leader remained %d", leader)
	}

	if !Replicate(tc.cluster[newLeader].rf, cmds[4]) {
		t.Errorf("expected %d to be leader", newLeader)
	}
	time.Sleep(250*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[4]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[4], n)
	}
	if nGot, _ := tc.Committed(cmds[3]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[3], n)
	}
	tc.NotCommitted(cmds[2])
}


func TestReplicateNoMajority(t *testing.T) {
	cmds, n := []int{0, 1, 2, 3, 4, 5}, 3
	tc := NewTestCluster(n, t)
	defer tc.KillCluster()

	leader, term := tc.FindLeader(t)
	if !Replicate(tc.cluster[leader].rf, cmds[0]) {
		t.Errorf("expected %d to be leader", leader)
	}
	if !Replicate(tc.cluster[leader].rf, cmds[1]) {
		t.Errorf("expected %d to be leader", leader)
	}

	time.Sleep(450*time.Millisecond)
	if nGot, _ := tc.Committed(cmds[1]); nGot != n {
		t.Errorf("%d servers committed cmd %d, expected %d servers to commit this cmd", nGot, cmds[1], n)
	}

	other1 := (leader + 1) % n
	other2 := (other1 + 1) % n
	tc.Disconnect(other1)
	tc.Disconnect(other2)
	time.Sleep(450*time.Millisecond)

	if !Replicate(tc.cluster[leader].rf, cmds[2]) {
		t.Errorf("expected %d to be leader", leader)
	}
	time.Sleep(250*time.Millisecond)
	tc.NotCommitted(cmds[2])

	tc.Connect(other1)
	tc.Connect(other2)
	time.Sleep(450*time.Millisecond)

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