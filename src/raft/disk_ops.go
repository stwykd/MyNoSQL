package raft

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"log"
)

func (r *Raft) encode() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(r.currentTerm) != nil ||
		e.Encode(r.votedFor) != nil ||
		e.Encode(r.logIndex) != nil ||
		e.Encode(r.commitIndex) != nil ||
		e.Encode(r.lastApplied) != nil ||
		e.Encode(r.log) != nil {
		log.Fatal("error while marshaling raft state")
	}

	data := w.Bytes()
	return data
}

func (r *Raft) decode(data []byte) {
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
		d.Decode(&r.log) != nil {
		log.Fatal("error while unmarshaling raft state")
	}
	r.currentTerm, r.votedFor, r.logIndex, r.commitIndex, r.lastApplied =
		currentTerm, votedFor, logIndex, commitIndex, lastApplied
}

func (r *Raft) saveState() {
	ioutil.WriteFile("raft_state", r.encode(), 0777)
}

func (r *Raft) loadState() {
	data, _ := ioutil.ReadFile("raft_state")
	r.decode(data)
}
