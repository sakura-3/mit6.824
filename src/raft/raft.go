package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term int
	Cmd  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 持久化状态
	currentTerm int
	votedFor    int
	log         []LogEntry

	// 易失性状态
	commitIndex int
	lastApplied int

	// 易失性状态(针对leader)
	nextIndex  []int
	matchIndex []int

	// 其它
	role     Role
	voteTime time.Time
	applyCh  chan ApplyMsg
	snapShot []byte

	// 需要持久化
	LastIncludedIndex int
	LastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader && !rf.killed()
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)

	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	rf.persister.Save(w.Bytes(), rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor, currentTerm, lastIncludeIdx, lastIncludedTerm int
	var log []LogEntry

	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIdx) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		Debug(dPersist, "S%d,readPersist failed.", rf.me)
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log

		rf.LastIncludedIndex = lastIncludeIdx
		rf.LastIncludedTerm = lastIncludedTerm

		rf.lastApplied = max(rf.lastApplied, lastIncludeIdx)
		rf.commitIndex = max(rf.commitIndex, lastIncludeIdx)
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.snapShot = data
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.LastIncludedIndex {
		Debug(dSnap, "S%d reject snapshot before %d,commitIdx=%d,lastIncludeIdx=%d.", rf.me, index, rf.commitIndex, rf.LastIncludedIndex)
		return
	}

	Debug(dSnap, "S%d accept snapshot before %d.", rf.me, index)
	rf.snapShot = snapshot
	rf.LastIncludedTerm = rf.log[rf.realIndex(rf.LastIncludedIndex)].Term

	// 有效log仍然从1开始,直接在原切片上截取无法被gc
	newLog := make([]LogEntry, 1)
	newLog[0] = rf.log[rf.realIndex(index)]
	newLog = append(newLog, rf.log[rf.realIndex(rf.LastIncludedIndex+1):]...)
	rf.log = newLog

	rf.LastIncludedIndex = index

	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)

	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.killed() {
		return -1, -1, false
	}

	e := LogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, e)
	index = len(rf.log) - 1
	term = rf.currentTerm
	isLeader = true

	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.role = Follwer
	rf.votedFor = -1
	rf.voteTime = time.Now()
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 1_index
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.virtualIndex(len(rf.log))
	}
	// start ticker goroutine to start elections
	go rf.electTicker()
	go rf.appendTicker()
	go rf.commitTicker()

	return rf
}
