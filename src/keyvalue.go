package src

import (
	"log"
	"math/rand"
	"net"
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

func NewClient() *Client {
	c := new(Client)
	c.client = rand.Uint64()
	return c
}

func (c *Client) Connect(addr net.Addr) {
	server, err := rpc.Dial(addr.Network(), addr.String())
	if err != nil {
		log.Fatalf("error dialing server: %s", err)
	}
	c.server = server
}

func (c *Client) Get(k string) string {
	var reply GetReply
	for r:=0; r< retries; r++ {
		if err := c.server.Call("Server.Get", GetArgs{k}, &reply); err != nil {
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
	var reply PutReply
	for r:=0; r< retries; r++ {
		if err := c.server.Call("Server.Put", PutArgs{k, v}, &reply); err != nil {
			log.Fatalf("error while calling Server.Put RPC: %s", err.Error())
		}
		if reply.Success {
			return true
		}
		time.Sleep(RetryInterval)
	}
	return false
}