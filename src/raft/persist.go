package raft

import (
	"bytes"
	"encoding/gob"
	"log"
)

type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	Ready() bool
}
