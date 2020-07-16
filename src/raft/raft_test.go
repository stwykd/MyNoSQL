package raft

import (
	"testing"
	"time"
)

func TestElection(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	tc.FindLeader()
}

func TestLeaderDown(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, term := tc.FindLeader()

	tc.Disconnect(leader)
	time.Sleep(350*time.Millisecond)

	newLeader, newTerm := tc.FindLeader()
	if newLeader == leader {
		t.Errorf("leader didn't change after disconencting leader")
	}
	if newTerm <= term {
		t.Errorf("term didn't change after disconnecting leader")
	}
}


func TestReconnectLeader(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()
	leader, _ := tc.FindLeader()

	tc.Disconnect(leader)

	time.Sleep(350*time.Millisecond)
	newLeader, newTerm := tc.FindLeader()

	tc.Connect(leader)
	time.Sleep(150*time.Millisecond)

	newLeader_, newTerm_ := tc.FindLeader()

	if newLeader != newLeader_ {
		t.Errorf("leader changed from %d to %d", newLeader, newLeader_)
	}
	if newTerm != newTerm_ {
		t.Errorf("term changed from %d to %d", newTerm, newTerm_)
	}
}


func TestKeepReconnectingLeader(t *testing.T) {
	tc := NewTestCluster(t, 5)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()

	tc.Disconnect(leader)
	time.Sleep(150*time.Millisecond)
	newLeader, newTerm := tc.FindLeader()

	tc.Connect(leader)
	time.Sleep(150*time.Millisecond)

	newLeader_, newTerm_ := tc.FindLeader()

	if newLeader != newLeader_ {
		t.Errorf("leader changed from %d to %d", newLeader, newLeader_)
	}
	if newTerm != newTerm_ {
		t.Errorf("term changed from %d to %d", newTerm, newTerm_)
	}
}

func TestNoQuorum(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()

	// remove 2/3 servers
	tc.Disconnect(leader)
	other := (leader + 1) % 3
	tc.Disconnect(other)

	time.Sleep(450*time.Millisecond)
	tc.NoQuorum()

	tc.Connect(other)
	tc.FindLeader()
}

func TestRestartCluster(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	time.Sleep(100*time.Millisecond)
	for i := 0; i < 3; i++ {
		tc.Disconnect(i)
	}
	time.Sleep(450*time.Millisecond)
	tc.NoQuorum()

	for i := 0; i < 3; i++ {
		tc.Connect(i)
	}
	tc.FindLeader()
}

func TestReconnectFollower(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, origTerm := tc.FindLeader()

	other := (leader + 1) % 3
	tc.Disconnect(other)
	time.Sleep(650 * time.Millisecond)
	tc.Connect(other)
	time.Sleep(150*time.Millisecond)

	_, newTerm := tc.FindLeader()
	if newTerm <= origTerm {
		t.Errorf("newTerm <= term after reconnecting follower")
	}
}

func TestReconnectLeader5(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	for reconnects := 0; reconnects < 5; reconnects++ {
		leader, _ := tc.FindLeader()

		tc.Disconnect(leader)
		other := (leader + 1) % 3
		tc.Disconnect(other)
		time.Sleep(310*time.Millisecond)
		tc.NoQuorum()

		tc.Connect(other)
		tc.Connect(leader)

		time.Sleep(150*time.Millisecond)
	}
}

func TestReplicate(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()

	isLeader := tc.Replicate(leader, 42)
	if !isLeader {
		t.Errorf("expected %d to be leader", leader)
	}

	time.Sleep(250*time.Millisecond)
	tc.CommittedN(42, 3)
}

func TestReplicateMore(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()

	values := []int{42, 55, 81}
	for _, v := range values {
		if !tc.Replicate(leader, v) {
			t.Errorf("expected %d to be leader", leader)
		}
		time.Sleep(100*time.Millisecond)
	}

	time.Sleep(250*time.Millisecond)
	nc, i1 := tc.Committed(42)
	_, i2 := tc.Committed(55)
	if nc != 3 {
		t.Errorf("want nc=3, got %d", nc)
	}
	if i1 >= i2 {
		t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	}

	_, i3 := tc.Committed(81)
	if i2 >= i3 {
		t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	}
}

func TestReplicateFollowerReconnect(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()
	other := (leader + 1) % 3
	if tc.Replicate(other, 42) {
		t.Errorf("didn't expect %d to be leader", other)
	}
	time.Sleep(10*time.Millisecond)
}

func TestReplicateReconnect(t *testing.T) {

	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	// Submit a couple of values to a fully connected cluster.
	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)

	time.Sleep(250*time.Millisecond)
	tc.CommittedN(6, 3)

	dPeerId := (leader + 1) % 3
	tc.Disconnect(dPeerId)
	time.Sleep(250*time.Millisecond)

	// Submit a new command; it will be committed but only to two servers.
	tc.Replicate(leader, 7)
	time.Sleep(250*time.Millisecond)
	tc.CommittedN(7, 2)

	// Now reconnect dPeerId and wait a bit; it should find the new command too.
	tc.Connect(dPeerId)
	time.Sleep(250*time.Millisecond)
	tc.FindLeader()

	time.Sleep(150*time.Millisecond)
	tc.CommittedN(7, 3)
}

func TestReplicateReconnectLeader(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	// Submit a couple of values to a fully connected cluster.
	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)
	time.Sleep(250*time.Millisecond)
	tc.CommittedN(6, 3)

	// Disconnect leader for a short time (less than election timeout in peers).
	tc.Disconnect(leader)
	time.Sleep(90*time.Millisecond)
	tc.Connect(leader)
	time.Sleep(200*time.Millisecond)

	tc.Replicate(leader, 7)
	time.Sleep(250*time.Millisecond)
	tc.CommittedN(7, 3)
}

func TestReplicateNoQuorum(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	// Submit a couple of values to a fully connected cluster.
	leader, origTerm := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)

	time.Sleep(250*time.Millisecond)
	tc.CommittedN(6, 3)

	// Disconnect both followers.
	dPeer1 := (leader + 1) % 3
	dPeer2 := (leader + 2) % 3
	tc.Disconnect(dPeer1)
	tc.Disconnect(dPeer2)
	time.Sleep(250*time.Millisecond)

	tc.Replicate(leader, 8)
	time.Sleep(250*time.Millisecond)
	tc.NotCommitted(8)

	tc.Connect(dPeer1)
	tc.Connect(dPeer2)
	time.Sleep(600*time.Millisecond)

	tc.NotCommitted(8)

	newLeaderId, againTerm := tc.FindLeader()
	if origTerm == againTerm {
		t.Errorf("got origTerm==againTerm==%d; want them different", origTerm)
	}

	// But new values will be committed for sure...
	tc.Replicate(newLeaderId, 9)
	tc.Replicate(newLeaderId, 10)
	tc.Replicate(newLeaderId, 11)
	time.Sleep(350*time.Millisecond)

	for _, v := range []int{9, 10, 11} {
		tc.CommittedN(v, 3)
	}
}

func TestReplicateReconnectLeader5(t *testing.T) {
	tc := NewTestCluster(t, 5)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)

	time.Sleep(250*time.Millisecond)
	tc.CommittedN(6, 5)

	// Leader disconnected...
	tc.Disconnect(leader)
	time.Sleep(10*time.Millisecond)

	tc.Replicate(leader, 7)

	time.Sleep(250*time.Millisecond)
	tc.NotCommitted(7)

	newLeaderId, _ := tc.FindLeader()

	// Submit 8 to new leader.
	tc.Replicate(newLeaderId, 8)
	time.Sleep(250*time.Millisecond)
	tc.CommittedN(8, 4)

	tc.Connect(leader)
	time.Sleep(600*time.Millisecond)

	finalLeaderId, _ := tc.FindLeader()
	if finalLeaderId == leader {
		t.Errorf("got finalLeaderId==leader==%d, want them different", finalLeaderId)
	}

	tc.Replicate(newLeaderId, 9)
	time.Sleep(250*time.Millisecond)
	tc.CommittedN(9, 5)
	tc.CommittedN(8, 5)

	// But 7 is not committed...
	tc.NotCommitted(7)
}

func TestFollowerCrash(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)

	time.Sleep(350*time.Millisecond)
	tc.CommittedN(5, 3)

	tc.Crash((leader + 1) % 3)
	time.Sleep(350*time.Millisecond)
	tc.CommittedN(5, 2)
}

func TestFollowerRestart(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)
	tc.Replicate(leader, 7)

	vals := []int{5, 6, 7}

	time.Sleep(350*time.Millisecond)
	for _, v := range vals {
		tc.CommittedN(v, 3)
	}

	tc.Crash((leader + 1) % 3)
	time.Sleep(350*time.Millisecond)
	for _, v := range vals {
		tc.CommittedN(v, 2)
	}

	tc.Restart((leader + 1) % 3)
	time.Sleep(650*time.Millisecond)
	for _, v := range vals {
		tc.CommittedN(v, 3)
	}
}

func TestLeaderRestart(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)
	tc.Replicate(leader, 7)

	vals := []int{5, 6, 7}

	time.Sleep(350*time.Millisecond)
	for _, v := range vals {
		tc.CommittedN(v, 3)
	}

	tc.Crash(leader)
	time.Sleep(350*time.Millisecond)
	for _, v := range vals {
		tc.CommittedN(v, 2)
	}

	tc.Restart(leader)
	time.Sleep(550*time.Millisecond)
	for _, v := range vals {
		tc.CommittedN(v, 3)
	}
}

func TestClusterRestart(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)
	tc.Replicate(leader, 7)

	vals := []int{5, 6, 7}

	time.Sleep(350*time.Millisecond)
	for _, v := range vals {
		tc.CommittedN(v, 3)
	}

	for i := 0; i < 3; i++ {
		tc.Crash((leader + i) % 3)
	}

	time.Sleep(350*time.Millisecond)

	for i := 0; i < 3; i++ {
		tc.Restart((leader + i) % 3)
	}

	time.Sleep(150*time.Millisecond)
	newLeaderId, _ := tc.FindLeader()

	tc.Replicate(newLeaderId, 8)
	time.Sleep(250*time.Millisecond)

	vals = []int{5, 6, 7, 8}
	for _, v := range vals {
		tc.CommittedN(v, 3)
	}
}

func TestReplaceLogEntries(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()
	tc.Replicate(leader, 5)
	tc.Replicate(leader, 6)

	time.Sleep(250*time.Millisecond)
	tc.CommittedN(6, 3)

	// Leader disconnected...
	tc.Disconnect(leader)
	time.Sleep(10*time.Millisecond)

	tc.Replicate(leader, 21)
	time.Sleep(5*time.Millisecond)
	tc.Replicate(leader, 22)
	time.Sleep(5*time.Millisecond)
	tc.Replicate(leader, 23)
	time.Sleep(5*time.Millisecond)
	tc.Replicate(leader, 24)
	time.Sleep(5*time.Millisecond)

	newLeaderId, _ := tc.FindLeader()

	tc.Replicate(newLeaderId, 8)
	time.Sleep(5*time.Millisecond)
	tc.Replicate(newLeaderId, 9)
	time.Sleep(5*time.Millisecond)
	tc.Replicate(newLeaderId, 10)
	time.Sleep(250*time.Millisecond)
	tc.NotCommitted(21)
	tc.CommittedN(10, 2)

	tc.Crash(newLeaderId)
	time.Sleep(60*time.Millisecond)
	tc.Restart(newLeaderId)

	time.Sleep(100*time.Millisecond)
	finalLeaderId, _ := tc.FindLeader()
	tc.Connect(leader)
	time.Sleep(400*time.Millisecond)

	tc.Replicate(finalLeaderId, 11)
	time.Sleep(250*time.Millisecond)

	tc.NotCommitted(21)
	tc.CommittedN(11, 3)
	tc.CommittedN(10, 3)
}

func TestReplicateThenCrash(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.KillCluster()

	leader, _ := tc.FindLeader()

	tc.Replicate(leader, 5)
	time.Sleep(1*time.Millisecond)
	tc.Crash(leader)

	time.Sleep(10*time.Millisecond)
	tc.FindLeader()
	time.Sleep(300*time.Millisecond)
	tc.NotCommitted(5)

	tc.Restart(leader)
	time.Sleep(150*time.Millisecond)
	newLeaderId, _ := tc.FindLeader()
	tc.NotCommitted(5)

	tc.Replicate(newLeaderId, 6)
	time.Sleep(100*time.Millisecond)
	tc.CommittedN(5, 3)
	tc.CommittedN(6, 3)
}
