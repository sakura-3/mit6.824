package raft

import "time"

type Role int

const (
	Leader Role = iota
	Follwer
	Candidate
)

const (
	HeartbeatInteval  int64 = 35
	MinElectInteval   int64 = 75
	ExtraElectInteval int64 = 100
)

// becomeRole expected rf.mu locked

func (rf *Raft) becomeLeader(term int) {
	Debug(dInfo, "S%d become leader at T%d.", rf.me, rf.currentTerm)

	rf.role = Leader
	rf.mu.Unlock()
	go rf.leaderAppendEntries()
	rf.mu.Lock()
}

func (rf *Raft) becomeFollower(term int) {
	Debug(dInfo, "S%d become follwer,T%d -> T%d.", rf.me, rf.currentTerm, term)

	rf.currentTerm = term
	rf.role = Follwer
	rf.votedFor = -1
	rf.voteTime = time.Now()
}

func (rf *Raft) becomeCandidate(term int) {
	Debug(dInfo, "S%d become Candidate,T%d -> T%d.", rf.me, rf.currentTerm, rf.currentTerm+1)

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteTime = time.Now()
}
