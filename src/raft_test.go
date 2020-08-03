package src

import (
	"log"
	"testing"
	"time"
)

func TestElection(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	CheckLeader(tc, t)
}

func TestLeaderDown(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, term := CheckLeader(tc, t)

	tc.Disconnect(leader)
	time.Sleep(350*time.Millisecond)

	newLeader, newTerm := CheckLeader(tc, t)
	if newLeader == leader {
		t.Errorf("leader didn't change after disconencting leader")
	}
	if newTerm <= term {
		t.Errorf("term didn't change after disconnecting leader")
	}
}


func TestReconnectLeader(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()
	leader, _ := CheckLeader(tc, t)

	tc.Disconnect(leader)

	time.Sleep(350*time.Millisecond)
	newLeader, newTerm := CheckLeader(tc, t)

	tc.Connect(leader)
	time.Sleep(150*time.Millisecond)

	newLeader_, newTerm_ := CheckLeader(tc, t)

	if newLeader != newLeader_ {
		t.Errorf("leader changed from %d to %d", newLeader, newLeader_)
	}
	if newTerm != newTerm_ {
		t.Errorf("term changed from %d to %d", newTerm, newTerm_)
	}
}


func TestKeepReconnectingLeader(t *testing.T) {
	n := 5
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)

	tc.Disconnect(leader)
	time.Sleep(150*time.Millisecond)
	newLeader, newTerm := CheckLeader(tc, t)

	tc.Connect(leader)
	time.Sleep(150*time.Millisecond)

	newLeader_, newTerm_ := CheckLeader(tc, t)

	if newLeader != newLeader_ {
		t.Errorf("leader changed from %d to %d", newLeader, newLeader_)
	}
	if newTerm != newTerm_ {
		t.Errorf("term changed from %d to %d", newTerm, newTerm_)
	}
}

func TestNoQuorum(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)

	tc.Disconnect(leader)
	other := (leader + 1) % n
	tc.Disconnect(other)

	time.Sleep(450*time.Millisecond)
	NoQuorum(tc, t)

	tc.Connect(other)
	CheckLeader(tc, t)
}

func TestRestartCluster(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	time.Sleep(100*time.Millisecond)
	for i := 0; i < n; i++ {
		tc.Disconnect(i)
	}
	time.Sleep(450*time.Millisecond)
	NoQuorum(tc, t)

	for i := 0; i < n; i++ {
		tc.Connect(i)
	}
	CheckLeader(tc, t)
}

func TestReconnectFollower(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, origTerm := CheckLeader(tc, t)

	other := (leader + 1) % n
	tc.Disconnect(other)
	time.Sleep(650 * time.Millisecond)
	tc.Connect(other)
	time.Sleep(150*time.Millisecond)

	_, newTerm := CheckLeader(tc, t)
	if newTerm <= origTerm {
		t.Errorf("newTerm <= term after reconnecting follower")
	}
}

func TestReconnectLeader5(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	for reconnects := 0; reconnects < 5; reconnects++ {
		leader, _ := CheckLeader(tc, t)

		tc.Disconnect(leader)
		other := (leader + 1) % n
		tc.Disconnect(other)
		time.Sleep(310*time.Millisecond)
		NoQuorum(tc, t)

		tc.Connect(other)
		tc.Connect(leader)

		time.Sleep(150*time.Millisecond)
	}
}

func TestReplicate(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)

	isLeader := Replicate(tc, t, leader, 42)
	if !isLeader {
		t.Errorf("expected %d to be leader", leader)
	}

	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 42, n)
}

func TestReplicateMore(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)

	cmds := []int{42, 55, 81}
	for _, v := range cmds {
		if !Replicate(tc, t, leader, v) {
			t.Errorf("expected %d to be leader", leader)
		}
		time.Sleep(100*time.Millisecond)
	}

	time.Sleep(250*time.Millisecond)
	nc, i1 := Committed(tc, t, 42)
	_, i2 := Committed(tc, t, 55)
	if nc != n {
		t.Errorf("want nc=3, got %d", nc)
	}
	if i1 >= i2 {
		t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	}

	_, i3 := Committed(tc, t, 81)
	if i2 >= i3 {
		t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	}
}

func TestReplicateFollowerReconnect(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	other := (leader + 1) % n
	if Replicate(tc, t, other, 1) {
		t.Errorf("didn't expect %d to be leader", other)
	}
	time.Sleep(10*time.Millisecond)
}

func TestReplicateReconnect(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)

	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 6, n)

	other := (leader + 1) % n
	tc.Disconnect(other)
	time.Sleep(250*time.Millisecond)

	Replicate(tc, t, leader, 7)
	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 7, n-1)

	tc.Connect(other)
	time.Sleep(250*time.Millisecond)
	CheckLeader(tc, t)

	time.Sleep(150*time.Millisecond)
	CommittedN(tc, t, 7, n)
}

func TestReplicateReconnectLeader(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)
	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 6, n)

	tc.Disconnect(leader)
	time.Sleep(90*time.Millisecond)
	tc.Connect(leader)
	time.Sleep(200*time.Millisecond)

	Replicate(tc, t, leader, 7)
	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 7, n)
}

func TestReplicateNoQuorum(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, origTerm := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)

	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 6, n)

	other1 := (leader + 1) % n
	other2 := (leader + 2) % n
	tc.Disconnect(other1)
	tc.Disconnect(other2)
	time.Sleep(250*time.Millisecond)

	Replicate(tc, t, leader, 8)
	time.Sleep(250*time.Millisecond)
	NotCommitted(tc, t, 8)

	tc.Connect(other1)
	tc.Connect(other2)
	time.Sleep(600*time.Millisecond)

	NotCommitted(tc, t, 8)

	newLeaderId, againTerm := CheckLeader(tc, t)
	if origTerm == againTerm {
		t.Errorf("got origTerm==againTerm==%d; want them different", origTerm)
	}

	Replicate(tc, t, newLeaderId, 9)
	Replicate(tc, t, newLeaderId, 10)
	Replicate(tc, t, newLeaderId, 11)
	time.Sleep(350*time.Millisecond)

	for _, v := range []int{9, 10, 11} {
		CommittedN(tc, t, v, n)
	}
}

func TestReplicateReconnectLeader5(t *testing.T) {
	n:=5
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)

	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 6, n)

	tc.Disconnect(leader)
	time.Sleep(10*time.Millisecond)

	Replicate(tc, t, leader, 7)

	time.Sleep(250*time.Millisecond)
	NotCommitted(tc, t, 7)

	newLeaderId, _ := CheckLeader(tc, t)

	Replicate(tc, t, newLeaderId, 8)
	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 8, n-1)

	tc.Connect(leader)
	time.Sleep(600*time.Millisecond)

	finalLeaderId, _ := CheckLeader(tc, t)
	if finalLeaderId == leader {
		t.Errorf("got finalLeaderId==leader==%d, want them different", finalLeaderId)
	}

	Replicate(tc, t, newLeaderId, 9)
	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 9, n)
	CommittedN(tc, t, 8, n)

	NotCommitted(tc, t, 7)
}

func TestFollowerCrash(t *testing.T) {
	n:=3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)

	time.Sleep(350*time.Millisecond)
	CommittedN(tc, t, 5, n)

	Crash(tc, t, (leader + 1) % 3)
	time.Sleep(350*time.Millisecond)
	CommittedN(tc, t, 5, n-1)
}

func TestFollowerRestart(t *testing.T) {
	n:=3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)
	Replicate(tc, t, leader, 7)

	cmds := []int{5, 6, 7}

	time.Sleep(350*time.Millisecond)
	for _, v := range cmds {
		CommittedN(tc, t, v, n)
	}

	Crash(tc, t, (leader + 1) % n)
	time.Sleep(350*time.Millisecond)
	for _, v := range cmds {
		CommittedN(tc, t, v, n-1)
	}

	Restart(tc, t, (leader + 1) % n)
	time.Sleep(650*time.Millisecond)
	for _, v := range cmds {
		CommittedN(tc, t, v, n)
	}
}

func TestLeaderRestart(t *testing.T) {
	n:=3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)
	Replicate(tc, t, leader, 7)

	cmds := []int{5, 6, 7}

	time.Sleep(350*time.Millisecond)
	for _, v := range cmds {
		CommittedN(tc, t, v, n)
	}

	Crash(tc, t, leader)
	time.Sleep(350*time.Millisecond)
	for _, v := range cmds {
		CommittedN(tc, t, v, n-1)
	}

	Restart(tc, t, leader)
	time.Sleep(550*time.Millisecond)
	for _, v := range cmds {
		CommittedN(tc, t, v, n)
	}
}

func TestClusterRestart(t *testing.T) {
	n:=3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)
	Replicate(tc, t, leader, 7)

	cmds := []int{5, 6, 7}

	time.Sleep(350*time.Millisecond)
	for _, v := range cmds {
		CommittedN(tc, t, v, n)
	}

	for i := 0; i < 3; i++ {
		Crash(tc, t, (leader + i) % n)
	}

	time.Sleep(350*time.Millisecond)

	for i := 0; i < 3; i++ {
		Restart(tc, t, (leader + i) % n)
	}

	time.Sleep(150*time.Millisecond)
	newLeaderId, _ := CheckLeader(tc, t)

	Replicate(tc, t, newLeaderId, 8)
	time.Sleep(250*time.Millisecond)

	cmds = []int{5, 6, 7, 8}
	for _, v := range cmds {
		CommittedN(tc, t, v, n)
	}
}

func TestReplaceLogEntries(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)
	Replicate(tc, t, leader, 5)
	Replicate(tc, t, leader, 6)

	time.Sleep(250*time.Millisecond)
	CommittedN(tc, t, 6, n)

	tc.Disconnect(leader)
	time.Sleep(10*time.Millisecond)

	Replicate(tc, t, leader, 21)
	time.Sleep(5*time.Millisecond)
	Replicate(tc, t, leader, 22)
	time.Sleep(5*time.Millisecond)
	Replicate(tc, t, leader, 23)
	time.Sleep(5*time.Millisecond)
	Replicate(tc, t, leader, 24)
	time.Sleep(5*time.Millisecond)

	newLeaderId, _ := CheckLeader(tc, t)

	Replicate(tc, t, newLeaderId, 8)
	time.Sleep(5*time.Millisecond)
	Replicate(tc, t, newLeaderId, 9)
	time.Sleep(5*time.Millisecond)
	Replicate(tc, t, newLeaderId, 10)
	time.Sleep(250*time.Millisecond)
	NotCommitted(tc, t, 21)
	CommittedN(tc, t, 10, n-1)

	Crash(tc, t, newLeaderId)
	time.Sleep(60*time.Millisecond)
	Restart(tc, t, newLeaderId)

	time.Sleep(100*time.Millisecond)
	finalLeaderId, _ := CheckLeader(tc, t)
	tc.Connect(leader)
	time.Sleep(400*time.Millisecond)

	Replicate(tc, t, finalLeaderId, 11)
	time.Sleep(250*time.Millisecond)

	NotCommitted(tc, t, 21)
	CommittedN(tc, t, 11, n)
	CommittedN(tc, t, 10, n)
}

func TestReplicateThenCrash(t *testing.T) {
	n := 3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leader, _ := CheckLeader(tc, t)

	Replicate(tc, t, leader, 5)
	time.Sleep(1*time.Millisecond)
	Crash(tc, t, leader)

	time.Sleep(10*time.Millisecond)
	CheckLeader(tc, t)
	time.Sleep(300*time.Millisecond)
	NotCommitted(tc, t, 5)

	Restart(tc, t, leader)
	time.Sleep(150*time.Millisecond)
	newLeaderId, _ := CheckLeader(tc, t)
	NotCommitted(tc, t, 5)

	Replicate(tc, t, newLeaderId, 6)
	time.Sleep(100*time.Millisecond)
	CommittedN(tc, t, 5, n)
	CommittedN(tc, t, 6, n)
}





// Test helpers

// == Election ==

func CheckLeader(tc *Cluster, t *testing.T) (int, int) {
	for r := 0; r < 8; r++ {
		leaderId, leaderTerm := -1, -1
		for i := 0; i < tc.n; i++ {
			if tc.connected[i] {
				tc.cluster[i].rf.mu.Lock()
				term, isLeader := tc.cluster[i].rf.currentTerm, tc.cluster[i].rf.state == Leader
				tc.cluster[i].rf.mu.Unlock()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	t.Fatalf("no leader elected")
	return -1, -1
}

// NoQuorum checks that no connected server considers itself the leader.
func NoQuorum(tc *Cluster, t *testing.T) {
	for id := 0; id < tc.n; id++ {
		if tc.connected[id] {
			tc.cluster[id].rf.mu.Lock()
			tc.cluster[id].rf.mu.Unlock()
			if tc.cluster[id].rf.state == Leader {
				t.Fatalf("%d became a leader without quorum", tc.cluster[id].rf.me)
			}
		}
	}
}


// == Replication ==

func Committed(tc *Cluster, t *testing.T, cmd int) (nc int, index int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	nCommits := -1
	for id := 0; id < tc.n; id++ {
		if tc.connected[id] {
			if nCommits >= 0 {
				if len(tc.commits[id]) != nCommits {
					t.Fatalf("num commits sent to client differs amongst servers")
				}
			} else {
				nCommits = len(tc.commits[id])
			}
		}
	}

	for c := 0; c < nCommits; c++ {
		expCmd := -1
		for id := 0; id < tc.n; id++ {
			if tc.connected[id] {
				gotCmd := tc.commits[id][c].Command.(int)
				if expCmd >= 0 {
					if gotCmd != expCmd {
						t.Errorf("got %d, want %d at tc.commits[%d][%d]", gotCmd, expCmd, id, c)
					}
				} else {
					expCmd = gotCmd
				}
			}
		}
		if expCmd == cmd {
			expIdx := -1
			nServers := 0
			for i := 0; i < tc.n; i++ {
				if tc.connected[i] {
					gotIdx := tc.commits[i][c].Index
					if expIdx >= 0 && gotIdx != expIdx {
						t.Errorf("server %d committed idx %d for cmd %d whilst server %d committed idx %d"+
							" for the same cmd", 0, expIdx, cmd, i, gotIdx)
					} else {
						expIdx = tc.commits[i][c].Index
					}
					nServers++
				}
			}
			return nServers, expIdx
		}
	}

	t.Errorf("command %d not amongst commits", cmd)
	return -1, -1
}

// CommittedN verifies that cmd was committed n times
func CommittedN(tc *Cluster, t *testing.T, cmd int, n int) {
	if nCommitted, _ := Committed(tc, t, cmd); nCommitted != n {
		t.Errorf("%d servers committed %d, expected %d servers to commit it", nCommitted, cmd, n)
	}
}

// NotCommitted verifies that cmd was not committed by any cluster server
func NotCommitted(tc *Cluster, t *testing.T, cmd int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for id := 0; id < tc.n; id++ {
		if tc.connected[id] {
			for c := 0; c < len(tc.commits[id]); c++ {
				gotCmd := tc.commits[id][c].Command.(int)
				if gotCmd == cmd {
					t.Errorf("found %d at commits[%d][%d], expected none", cmd, id, c)
				}
			}
		}
	}
}

func Replicate(tc *Cluster, t *testing.T, id int, cmd interface{}) bool {
	return tc.cluster[id].rf.Replicate(cmd)
}

// == Persist ==

// Crash crashes a server, however it's state is expected to be persisted
func Crash(tc *Cluster, t *testing.T, id int) {
	log.Printf("[%v] server crashing", id)
	tc.Disconnect(id)
	tc.alive[id] = false
	tc.cluster[id].Kill()

	tc.mu.Lock()
	tc.commits[id] = tc.commits[id][:0]
	tc.mu.Unlock()
}

// Restart restarts previously crashed server
func Restart(tc *Cluster, t *testing.T, id int) {
	if tc.alive[id] {
		log.Fatalf("me=%d is alive in Restart", id)
	}
	log.Printf("[%v] server starting", id)

	peerIds := make([]int, 0)
	for p := 0; p < tc.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan interface{})
	tc.cluster[id] = NewServer(id, peerIds, tc.storage[id], ready, tc.clientChs[id])
	tc.cluster[id].Run()
	tc.Connect(id)
	close(ready)
	tc.alive[id] = true
	time.Sleep(20*time.Millisecond)
}