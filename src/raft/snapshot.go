package raft

type InstallSnapshotArgs struct {
	Data []byte

	Term              int
	Leader            int
	LastIncludedIndex int
	LastIncludedTerm  int
	LastIncludedCmd   interface{} // 占位
}

type InstallSnapshotReply struct {
	Term int
}

// expected rf.mu locked
func (rf *Raft) realIndex(vdx int) int {
	return vdx - rf.LastIncludedIndex
}

func (rf *Raft) virtualIndex(rdx int) int {
	return rdx + rf.LastIncludedIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 1. reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d reject snapshot from %d,lower term(%d<%d)", rf.me, args.Leader, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.LastIncludedIndex < rf.LastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		Debug(dSnap, "S%d old snapshot from %d", rf.me, args.Leader)
		return
	}

	// 不考虑分片

	// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	rdx := args.LastIncludedIndex - rf.LastIncludedIndex
	has := rdx >= 1 && rdx < len(rf.log) && rf.log[rdx].Term == args.LastIncludedTerm

	if has {
		Debug(dSnap, "S%d retain log following [vdx=%d,rdx=%d]", rf.me, args.LastIncludedIndex, rdx)
		rf.log = append(make([]LogEntry, 0), rf.log[rdx:]...)
	} else {
		Debug(dSnap, "S%d discard log", rf.me)
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{Term: args.LastIncludedTerm, Cmd: args.LastIncludedCmd}
	}

	// 7.discard the entire log
	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.snapShot = args.Data
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.persist()

	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) handleInstallSnapshot(to int) {
	rf.mu.Lock()

	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		Leader:            rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		LastIncludedCmd:   rf.log[0].Cmd,
		Data:              rf.snapShot,
	}
	var reply InstallSnapshotReply

	rf.mu.Unlock()

	if ok := rf.peers[to].Call("Raft.InstallSnapshot", &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.role != Leader || rf.currentTerm != args.Term {
			Debug(dSnap, "S%d role or term changed", rf.me)
			return
		}

		if rf.currentTerm < reply.Term {
			Debug(dSnap, "S%d receive higher term in snapshot reply(%d>%d).", rf.me, reply.Term, rf.currentTerm)
			rf.becomeFollower(reply.Term)
			return
		}

		rf.matchIndex[to] = max(rf.matchIndex[to], args.LastIncludedIndex)
		rf.nextIndex[to] = rf.matchIndex[to] + 1
	}
}
