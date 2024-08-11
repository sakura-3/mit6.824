package raft

type InstallSnapshotArgs struct {
	Data []byte

	Term              int
	Leader            int
	LastIncludedIndex int
	LastIncludedTerm  int
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
