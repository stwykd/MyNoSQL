package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
)

// Storage is an interface implemented by stable storage providers.
type Storage interface {
	Set(key string, value []byte)

	Get(key string) ([]byte, bool)

	// Ready returns true iff any Sets were made on this Storage.
	Ready() bool
}

// TestStorage is a simple in-memory implementation of Storage for testing.
type TestStorage struct {
	mu sync.Mutex
	st map[string][]byte
}

func (rf *Raft) restore() {
	if termData, found := rf.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&rf.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := rf.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&rf.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := rf.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&rf.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

// persist saves all of CM's persistent state in rf.storage.
// Expects rf.mu to be locked.
func (rf *Raft) persist() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(rf.currentTerm); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(rf.votedFor); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(rf.log); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("log", logData.Bytes())
}


// dummy Storage used for testing

func NewTestStorage() *TestStorage {
	return &TestStorage{st: make(map[string][]byte)}
}

func (ms *TestStorage) Get(k string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.st[k]
	return v, found
}

func (ms *TestStorage) Set(k string, v []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.st[k] = v
}

func (ms *TestStorage) Ready() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.st) > 0
}

func getNTestStorage(n int) []Storage  {
	storage := make([]Storage, n)
	for i:=0; i<n; i++ {
		storage[i] = NewTestStorage()
	}
	return storage
}