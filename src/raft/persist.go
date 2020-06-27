package raft

import (
	"bytes"
	"encoding/gob"
	"log"
)

type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	Ready() bool // is new data available?
}

// restore Raft state from disk
// used to recover from server crash
func (rf *Raft) restore() {
	if encodedTerm, found := rf.storage.Get("currentTerm"); found {
		if err := gob.NewDecoder(bytes.NewBuffer(encodedTerm)).Decode(&rf.currentTerm); err != nil {
			log.Fatalf("[%v] %s", rf.me, err.Error())
		}
	} else {
		log.Fatalf("[%v] error restoring currentTerm", rf.me)
	}
	if encodedVoteFor, found := rf.storage.Get("votedFor"); found {
		if err := gob.NewDecoder(bytes.NewBuffer(encodedVoteFor)).Decode(&rf.votedFor); err != nil {
			log.Fatalf("[%v] %s", rf.me, err.Error())
		}
	} else {
		log.Fatalf("[%v] error restoring votedFor", rf.me)
	}
	if encodedLog, found := rf.storage.Get("log"); found {
		if err := gob.NewDecoder(bytes.NewBuffer(encodedLog)).Decode(&rf.log); err != nil {
			log.Fatalf("[%v] %s", rf.me, err.Error())
		}
	} else {
		log.Fatalf("[%v] error restoring log", rf.me)
	}
}

// persist Raft state to disk.
// called whenever any persisted variable (term, votedFor and log) changes
// expects rf.mu to be taken
func (rf *Raft) persist() {
	var encodedTerm bytes.Buffer
	if err := gob.NewEncoder(&encodedTerm).Encode(rf.currentTerm); err != nil {
		log.Fatalf("[%v] %s", rf.me, err.Error())
	}
	rf.storage.Set("currentTerm", encodedTerm.Bytes())

	var encodedVotedFor bytes.Buffer
	if err := gob.NewEncoder(&encodedVotedFor).Encode(rf.votedFor); err != nil {
		log.Fatalf("[%v] %s", rf.me, err.Error())
	}
	rf.storage.Set("votedFor", encodedVotedFor.Bytes())

	var encodedLog bytes.Buffer
	if err := gob.NewEncoder(&encodedLog).Encode(rf.log); err != nil {
		log.Fatalf("[%v] %s", rf.me, err.Error())
	}
	rf.storage.Set("log", encodedLog.Bytes())
}