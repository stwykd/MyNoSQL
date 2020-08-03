package src

import (
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

var RetryInterval = 1 * time.Second
var retries       = 3

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

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
	client   uint64 // client id
	server  *rpc.Client
}

func (c *Client) Client(server *rpc.Client) {
	c.client = rand.Uint64()
	c.server=server
}

func (c *Client) Get(k string) string {
	for r:=0; r< retries; r++ {
		var reply GetReply
		if err := c.server.Call("Server.Get", GetArgs{k}, reply); err != nil {
			log.Fatalf("error while calling Server.Get RPC: %s", err.Error())
		}
		time.Sleep(RetryInterval)
	}
	return ""
}

func (c *Client) Put(k, v string) bool {
	for r:=0; r< retries; r++ {
		var reply PutReply
		if err := c.server.Call("Server.Put", PutArgs{k, v}, reply); err != nil {
			log.Fatalf("error while calling Server.Put RPC: %s", err.Error())
		}
		time.Sleep(RetryInterval)
	}
	return false
}