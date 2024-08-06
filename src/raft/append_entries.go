package raft

import "time"

type (
	AppendEntriesArgs struct {
		Term         int
		Leader       int
		PrevLogIndex int
		PrevLogTerm  int
		Entries      []LogEntry
		LeaderCommit int
	}
	AppendEntriesReply struct {
		Term    int
		Success bool
	}
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = false
		return
	}

	rf.mu.Lock()
	if len(args.Entries) == 0 {
		Debug(dLog, "S%d <- S%d,Receive heartbeat at T%d.", rf.me, args.Leader, rf.currentTerm)
	} else {
		Debug(dLog, "S%d <- S%d,Receive append entries at T%d.", rf.me, args.Leader, rf.currentTerm)
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if rf.role != Follwer {
		rf.becomeFollower(args.Term)
	} else {
		rf.voteTime = time.Now()
	}

	reply.Success = true

	rf.mu.Unlock()
}

func (rf *Raft) leaderAppendEntries() {
	rf.mu.Lock()
	savedTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(to int) {
			rf.mu.Lock()

			if rf.role != Leader || rf.currentTerm != savedTerm {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{
				Term:   savedTerm,
				Leader: rf.me,
			}
			var reply AppendEntriesReply
			rf.mu.Unlock()

			Debug(dLog, "S%d -> S%d,Send append entries at T%d.", rf.me, to, savedTerm)

			if ok := rf.peers[to].Call("Raft.AppendEntries", &args, &reply); ok {
				rf.mu.Lock()
				Debug(dLog, "S%d <- S%d,Receive append entries reply at T%d.", rf.me, to, rf.currentTerm)
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					Debug(dLog, "S%d Receive higher term from S%d's append entries reply(%d > %d)", rf.me, to, reply.Term, rf.currentTerm)
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}
