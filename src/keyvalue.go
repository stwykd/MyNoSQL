package src

import (
	"log"
	"math/rand"
	"net/rpc"
)

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
	client  uint64 // client id
	leader  int
	servers []*rpc.Client
}

func NewClient(servers []*rpc.Client) *Client {
	c := new(Client)
	c.client = rand.Uint64()
	c.servers = servers
	return c
}

func (c *Client) Get(k string) string {
	var reply GetReply
	for r:=0; r<len(c.servers); r++ {
		if err := c.servers[c.leader].Call("Server.Get", GetArgs{k}, &reply); err != nil {
			log.Fatalf("error while calling Server.Get RPC: %s", err.Error())
		}
		if reply.Success {
			return reply.Value
		}
		c.leader=(c.leader+1)%len(c.servers)
	}
	return ""
}

func (c *Client) Put(k, v string) bool {
	var reply PutReply
	for s:=0; s<len(c.servers); s++ {
		if err := c.servers[c.leader].Call("Server.Put", PutArgs{k, v}, &reply); err != nil {
			log.Fatalf("error while calling Server.Put RPC: %s", err.Error())
		}
		if reply.Success {
			return true
		}
		c.leader=(c.leader+1)%len(c.servers)
	}
	return false
}