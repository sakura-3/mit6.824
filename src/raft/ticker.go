package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) electTimeout() time.Duration {
	return time.Duration(MinElectInteval+rand.Int63n(ExtraElectInteval)) * time.Millisecond
}

func (rf *Raft) electTicker() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C
		rf.mu.Lock()
		if elapsed := time.Since(rf.voteTime); elapsed >= rf.electTimeout() && rf.role != Leader {
			rf.becomeCandidate(rf.currentTerm)
			Debug(dTimer, "S%d's elect ticker elapsed,begin election.", rf.me)
			rf.mu.Unlock()
			go rf.elect(rf.currentTerm)
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) appendTicker() {
	ticker := time.NewTicker(time.Duration(HeartbeatInteval) * time.Millisecond)
	defer ticker.Stop()

	for !rf.killed() {

		<-ticker.C

		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			go rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) commitTicker() {
	ticker := time.NewTicker(time.Duration(CommitInteval) * time.Millisecond)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C

		rf.mu.Lock()
		savedApply := rf.lastApplied
		savedCommit := rf.commitIndex
		var buf []LogEntry
		if savedApply < rf.commitIndex {
			buf = make([]LogEntry, rf.commitIndex-savedApply)
			copy(buf, rf.log[savedApply+1:rf.commitIndex+1])
		}
		rf.mu.Unlock()

		// 异步提交
		for i, e := range buf {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: e.Cmd, CommandIndex: i + savedApply + 1}
		}
		Debug(dCommit, "S%d commit log[%d:%d]=%v", rf.me, savedApply+1, savedCommit+1, rf.log[savedApply+1:savedCommit+1])

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()
	}
}
