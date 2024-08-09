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
	MinElectInteval   int64 = 150
	ExtraElectInteval int64 = 100
	CommitInteval     int64 = 35
)

// becomeRole expected rf.mu locked

func (rf *Raft) becomeLeader(term int) {
	Debug(dInfo, "S%d become leader at T%d.", rf.me, rf.currentTerm)

	rf.role = Leader
	for i := range rf.peers {
		rf.matchIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	go rf.leaderAppendEntries()
}

func (rf *Raft) becomeFollower(term int) {
	Debug(dInfo, "S%d become follwer,T%d -> T%d.", rf.me, rf.currentTerm, term)

	rf.currentTerm = term
	rf.role = Follwer
	rf.votedFor = -1
	rf.voteTime = time.Now()

	rf.persist()
}

func (rf *Raft) becomeCandidate(term int) {
	Debug(dInfo, "S%d become Candidate,T%d -> T%d.", rf.me, rf.currentTerm, rf.currentTerm+1)

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteTime = time.Now()

	rf.persist()
}
