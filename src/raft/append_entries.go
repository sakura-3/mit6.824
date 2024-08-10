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

		// For backup optimization
		XTerm  int
		XIndex int
		XLen   int
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
		Debug(dLog, "S%d,receive append entries request with lower term(%d<%d),ignore.", rf.me, args.Term, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	if rf.role != Follwer {
		rf.becomeFollower(args.Term)
	} else {
		rf.voteTime = time.Now()
	}

	// if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	t := -1
	// 	if args.PrevLogIndex < len(rf.log) {
	// 		t = rf.log[args.PrevLogIndex].Term
	// 	}
	// 	Debug(dLog2, "S%d receive invalid prevLog[index=%d,term=%d],rf.log[len=%d,term=%d]", rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), t)
	// 	rf.mu.Unlock()
	// 	return
	// }

	if args.PrevLogIndex >= len(rf.log) {
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		reply.Success = false

		Debug(dLog2, "S%d:invalid prevlog,no entry exists at %d.", rf.me, args.PrevLogIndex)
		rf.mu.Unlock()
		return
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		t := args.PrevLogIndex - 1
		for t > 0 && rf.log[t].Term == rf.log[args.PrevLogIndex].Term {
			t--
		}
		reply.XIndex = t + 1
		reply.Success = false

		Debug(dLog2, "S%d:invalid prevlog,conflict at %d.", rf.me, args.PrevLogIndex)
		Debug(dTrace, "S%d's log=%v,args=%v", rf.me, rf.log, args)
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

				if rf.role != Leader || rf.currentTerm != savedTerm {
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
					rf.mu.Unlock()
					return
				}

				// !reply.Success,prevLog 不匹配
				Debug(dLog2, "S%d,log conflict at %d.", rf.me, args.PrevLogIndex)

				if reply.XTerm == -1 {
					Debug(dTrace, "S%d,follwer has no entries at %d,next[%d] back to %d.", rf.me, args.PrevLogIndex, to, reply.XLen)
					rf.nextIndex[to] = reply.XLen
					rf.mu.Unlock()
					return
				}

				// to已经被kill了，忽略可能导致next置0，并在下次append entry时导致越界(args.PrevLogIndex=-1)
				if reply.XTerm == 0 {
					rf.mu.Unlock()
					return
				}

				t := rf.nextIndex[to] - 1
				for t > 0 && rf.log[t].Term > reply.XTerm {
					t--
				}

				if rf.log[t].Term == reply.XTerm {
					Debug(dTrace, "S%d find entry with Xterm=%d at %d,next[%d] back to %d.", rf.me, reply.XTerm, t, to, t)
					Debug(dTrace, "S%d's log=%v,args=%+v,reply=%+v", rf.me, rf.log, args, reply)
					rf.nextIndex[to] = t
				} else {
					Debug(dTrace, "S%d has no entry with Xterm=%d,next[%d] back to %d.", rf.me, reply.XTerm, to, reply.XIndex)
					rf.nextIndex[to] = reply.XIndex
				}

				rf.mu.Unlock()
			}
		}(i)
	}
}
