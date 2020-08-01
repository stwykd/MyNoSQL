package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

var RetryInterval = 1 * time.Second
var retries       = 3

type PutArgs struct {
	Key        string
	Value      string
}

type PutReply struct {
	Success bool
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Success bool
	Value 	string
}

// Client of the key-value datastore
type Client struct {
	client  uint64 // client id
	server  *rpc.Client
	raftCh  <-chan Commit // raftCh notifies client when a command is committed
}

func (c *Client) Client(server *rpc.Client, raftCh <-chan Commit) {
	c.client = rand.Uint64()
	c.server=server
	c.raftCh=raftCh
}

func (c *Client) Get(k string) string {
	for r:=0; r<retries; r++ {
		var reply GetReply
		if err := c.server.Call("Server.Get", GetArgs{k}, reply); err != nil {
			log.Fatalf("error while calling Server.Get RPC: %s", err.Error())
		}
		if reply.Success {
			return reply.Value
		}
		time.Sleep(RetryInterval)
	}
	return ""
}

func (c *Client) Put(k, v string) bool {
	for r:=0; r<retries; r++ {
		var reply PutReply
		if err := c.server.Call("Server.Put", PutArgs{k, v}, reply); err != nil {
			log.Fatalf("error while calling Server.Put RPC: %s", err.Error())
		}
		if reply.Success {
			return true
		}
		time.Sleep(RetryInterval)
	}
	return false
}