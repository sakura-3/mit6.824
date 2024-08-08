package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) electTimeout() time.Duration {
	return time.Duration(MinElectInteval+rand.Int63n(ExtraElectInteval)) * time.Millisecond
}

func (rf *Raft) electTicker() {
	ticker := time.NewTicker(35 * time.Millisecond)
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
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Cmd,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- msg
		}

		if savedApply != rf.lastApplied {
			Debug(dCommit, "S%d commit log[%d:%d]=%v", rf.me, savedApply+1, rf.commitIndex+1, rf.log[savedApply+1:rf.commitIndex+1])
		}
		rf.mu.Unlock()
	}
}
