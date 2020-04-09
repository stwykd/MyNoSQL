package raft

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"log"
)

func (rf *Raft) encode() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.logIndex) != nil ||
		e.Encode(rf.commitIndex) != nil ||
		e.Encode(rf.lastApplied) != nil ||
		e.Encode(rf.log) != nil {
		log.Fatal("error while marshaling raft state")
	}

	data := w.Bytes()
	return data
}

func (rf *Raft) decode(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	d := gob.NewDecoder(bytes.NewBuffer(data))
	currentTerm, votedFor, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatal("error while unmarshaling raft state")
	}
	rf.currentTerm, rf.votedFor, rf.logIndex, rf.commitIndex, rf.lastApplied =
		currentTerm, votedFor, logIndex, commitIndex, lastApplied
}

func (rf *Raft) saveState() {
	ioutil.WriteFile("raft_state", rf.encode(), 0777)
}

func (rf *Raft) loadState() {
	data, _ := ioutil.ReadFile("raft_state")
	rf.decode(data)
}
