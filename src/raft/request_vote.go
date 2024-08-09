package raft

import (
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()
	Debug(dVote, "S%d <- S%d,Receive vote request at T%d.", rf.me, args.Candidate, rf.currentTerm)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d's term is lower,reject.(%d < %d).", args.Candidate, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		Debug(dVote, "S%d receive higher term from S%d's vote request(%d > %d).", rf.me, args.Candidate, args.Term, rf.currentTerm)
		rf.becomeFollower(args.Term)
	}

	n := len(rf.log)
	upToDate := args.LastLogTerm > rf.log[n-1].Term || (args.LastLogTerm == rf.log[n-1].Term && args.LastLogIndex >= n-1)

	if args.Term == rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.Candidate) && upToDate {
		Debug(dVote, "S%d vote for S%d at T%d.", rf.me, args.Candidate, rf.currentTerm)

		Debug(dTimer, "S%d,reset vote timer.", rf.me)
		rf.voteTime = time.Now()
		rf.votedFor = args.Candidate

		rf.persist()

		reply.VoteGranted = true
	} else if !upToDate {
		Debug(dVote, "S%d,vote request with older log from S%d,reject", rf.me, args.Candidate)
		reply.VoteGranted = false
	} else {
		Debug(dVote, "S%d already vote for %d at T%d.", rf.me, rf.votedFor, rf.currentTerm)
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) elect(term int) {
	Debug(dInfo, "S%d begin election.", rf.me)

	vote := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(to int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         term,
				Candidate:    rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()

			Debug(dVote, "S%d -> S%d,send vote request at T%d", rf.me, to, term)

			var reply RequestVoteReply
			if ok := rf.peers[to].Call("Raft.RequestVote", &args, &reply); ok {
				rf.mu.Lock()
				Debug(dVote, "S%d <- S%d,Receive vote reply at T%d,send at T%d.", rf.me, to, rf.currentTerm, term)

				if rf.role != Candidate || rf.currentTerm != term {
					Debug(dVote, "S%d's role or term changed,invalid vote.", rf.me)
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					Debug(dVote, "S%d Receive higher term from S%d's vote reply(%d > %d).", rf.me, to, reply.Term, rf.me)
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted {
					vote++
					if vote >= len(rf.peers)/2+1 {
						Debug(dVote, "S%d win the vote,begin at T%d.", rf.me, term)
						rf.becomeLeader(rf.currentTerm)
						rf.mu.Unlock()
						return
					}
				}

				rf.mu.Unlock()
			}
		}(i)
	}
}
