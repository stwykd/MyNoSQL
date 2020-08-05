package src

import (
	"testing"
	"time"
)


func TestPutGet(t *testing.T) {
	n:=3
	tc := NewCluster(getNTestStorage(n), n)
	time.Sleep(250*time.Millisecond)
	c := NewClient(tc.connectToServers())
	var k, v = "hello", "world"
	if !c.Put(k, v) {
		t.Fail()
	}

	if c.Get(k) != v {
		t.Fail()
	}
}