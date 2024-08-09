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

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		t := -1
		if args.PrevLogIndex < len(rf.log) {
			t = rf.log[args.PrevLogIndex].Term
		}
		Debug(dLog2, "S%d receive invalid prevLog[index=%d,term=%d],rf.log[len=%d,term=%d]", rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), t)
		rf.mu.Unlock()
		return
	}

	reply.Success = true

	for p, log := range args.Entries {
		q := args.PrevLogIndex + 1 + p
		if q < len(rf.log) && rf.log[q].Term != log.Term {
			Debug(dLog2, "S%d's log conflict at index_%d,[%d!=%d]", rf.me, q, rf.log[q].Term, args.Entries[p].Term)
			rf.log = rf.log[:q]
			rf.log = append(rf.log, args.Entries[p:]...)
			break
		} else if q == len(rf.log) {
			rf.log = append(rf.log, args.Entries[p:]...)
			break
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

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
				Debug(dLog, "S%d role or term changed,invalid append entries", rf.me)
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{
				Term:         savedTerm,
				Leader:       rf.me,
				PrevLogIndex: rf.nextIndex[to] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[to]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			if rf.nextIndex[to] < len(rf.log) {
				args.Entries = rf.log[rf.nextIndex[to]:]
			} else {
				args.Entries = make([]LogEntry, 0)
			}

			var reply AppendEntriesReply
			rf.mu.Unlock()

			Debug(dLog, "S%d -> S%d,Send append entries at T%d,log=%v", rf.me, to, savedTerm, args.Entries)

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

				if reply.Success {
					rf.matchIndex[to] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[to] = rf.matchIndex[to] + 1

					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						count := 1
						for i := range rf.peers {
							if i == rf.me {
								continue
							}
							if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = N
							break
						}
					}
				} else {
					// prevLog 不匹配
					rf.nextIndex[to]--
				}

				rf.mu.Unlock()
			}
		}(i)
	}
}
