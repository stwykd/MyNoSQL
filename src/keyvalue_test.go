package src

import (
	"testing"
)


func TestPutGet(t *testing.T) {
	n:=3
	tc := NewCluster(getNTestStorage(n), n)
	defer tc.KillCluster()

	leaderId, _ := CheckLeader(tc, t)
	leader := tc.cluster[leaderId]
	go leader.server.Accept(leader.listener)
	c := NewClient()
	c.Connect(leader.GetListenAddr())

	var k, v = "hello", "world"
	if !c.Put(k, v) {
		t.Fail()
	}

	if c.Get(k) != v {
		t.Fail()
	}
}