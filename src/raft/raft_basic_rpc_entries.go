package raft

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntries struct {
	// leader把它发送给其他server

	Term     int // leader 的term
	LeaderId int // leader 的id，每个follower用它来redirect客户的请求

	PrevLogIndex int // 紧邻新日志条目之前的那个日志条目的索引，对于每个follower，发的不一样
	PrevLogTerm  int // 紧邻新日志条目之前的那个日志条目的term，对于每个follower，发的不一样

	Entries      []*Entry // 日志内容
	LeaderCommit int      //	领导者的CommitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前follower见过的最新的任期
	Success bool // true 如果follower 获得了entry，并且 PrevLogIndex和PrevLogTerm都符合
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// resets the election timeout so that other servers don't step forward
// as leaders when one has already been elected
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	/*
			Receiver implementation:
		   1. Reply false if term < currentTerm (§5.1)
		   2. Reply false if log doesn’t contain an entry at prevLogIndex
		   whose term matches prevLogTerm (§5.3)
		   3. If an existing entry conflicts with a new one (same index
		   but different terms), delete the existing entry and all that
		   follow it (§5.3)
		   4. Append any new entries not already in the log
		   5. If leaderCommit > commitIndex, set commitIndex =
		   min(leaderCommit, index of last new entry)
	*/

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var success bool

	if args.Term < rf.CurrentTerm {
		// 1. Reply false if term < currentTerm (§5.1)
		DPrintf("[AppendEntries]: server %d send to %d, but server %d term %d is less than server %d term %d \n",
			args.LeaderId, rf.me, args.LeaderId, args.Term, rf.me, rf.CurrentTerm)
		success = false

	} else {
		if args.Term > rf.CurrentTerm {
			DPrintf("[AppendEntries]: server %d back to follower, args.Term %d, currentTerm: %d \n",
				rf.me, args.Term, rf.CurrentTerm,
			)
			rf.BackToFollower(args.Term)
			rf.ResetElectionTimer()
		}

		//  2. Reply false if log doesn’t contain an entry at prevLogIndex
		//	whose term matches prevLogTerm (§5.3)

		if args.PrevLogIndex <= len(rf.log)-1 {
			// 一致性检查失败
			if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				DPrintf("[AppendEntries]: server %d send to %d, prevLogIndex term  %d is not match %d \n",
					args.LeaderId, rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
				success = false
			} else {
				/*
					Another issue many had (often immediately after fixing the issue above), was that, upon receiving a heartbeat,
					they would truncate the follower’s log following prevLogIndex, and then append any entries included in
					the AppendEntries arguments. This is also not correct. We can once again turn to Figure 2:

					If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and
					all that follow it. The if here is crucial. If the follower has all the entries the leader sent,
					the follower MUST NOT truncate its log. Any elements following the entries sent by the leader MUST be kept.
					This is because we could be receiving an outdated AppendEntries RPC from the leader, and truncating the log
					would mean “taking back” entries that we may have already told the leader that we have in our log.
				*/

				/*
					3. If an existing entry conflicts with a new one (same index
					but different terms), delete the existing entry and all that
					follow it (§5.3)
				*/

				DPrintf("[AppendEntries]: server %d send to %d, prevLogIndex term  %d is match %d \n",
					args.LeaderId, rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

				// 这样也能通过测试，但是不要这么做最好
				//rf.log = rf.log[:args.PrevLogIndex+1]

				//
				for i := 0; i < len(args.Entries); i++ {
					index := args.PrevLogIndex + i + 1
					if index >= len(rf.log) {
						args.Entries = args.Entries[i:]
						rf.log = append(rf.log, args.Entries...)
						break
					} else if index < len(rf.log) {
						// check if conflict
						if rf.log[index].Term != args.Entries[i].Term {
							rf.log = rf.log[:index]
							args.Entries = args.Entries[i:]
							//  4. Append any new entries not already in the log
							rf.log = append(rf.log, args.Entries...)
							break
						}
					}
				}
				rf.persist()
				success = true
			}
		} else {
			/*
				ps: if the args' entry at previous index has the same term with the current one in logs
				but the latest one has conflicts

				3. If an existing entry conflicts with a new one (same index but different terms),
				   delete the existing entry and all that follow it (§5.3)
			*/
			// 如果leader的log更长，我的更短，甚至短于PrevLogIndex， leader要回退，

			// args.PrevLogIndex+1 is the latest entry to be appended
			// leader 发送的是从nextIndex到自己最新的log的所有entry
			// args.PrevLogIndex+1+i 是leader， 一条entry中真正的index
			success = false
		}
	}

	if success {

		rf.ResetElectionTimer()

		//	5. If leaderCommit > commitIndex, set commitIndex =
		//	min(leaderCommit, index of last new entry)
		if rf.CommitIndex < args.LeaderCommit {
			if args.LeaderCommit > len(rf.log)-1 {
				rf.CommitIndex = len(rf.log) - 1
			} else {
				rf.CommitIndex = args.LeaderCommit
			}
			rf.applyCond.Broadcast()
		}

		reply.Term = rf.CurrentTerm
		reply.Success = success
	} else {
		//DPrintf("AppendEntries: server %d refused hb, and dont reset ResetElection \n", rf.me)
		reply.Term = rf.CurrentTerm
		reply.Success = success
	}
}

func (rf *Raft) SendAppendEntriesToAll(name string) {
	nReplica := 1

	majority := len(rf.peers)/2 + 1

	rf.mu.Lock()
	CurrentTerm := rf.CurrentTerm
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		k := i

		go func(i int, nReplica *int) {

		retry:
			args := AppendEntries{}
			args.Term = CurrentTerm
			args.LeaderId = rf.me

			rf.mu.Lock()
			args.PrevLogIndex = rf.NextIndex[i] - 1
			if args.PrevLogIndex < 0 {
				DPrintf("[AppendEntries_%s]: Server %d term %d expired, skip retry with nextIndex %d", name, rf.me, CurrentTerm, rf.NextIndex[i])
				rf.mu.Unlock()
				return
			}
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[rf.NextIndex[i]:]
			args.LeaderCommit = rf.CommitIndex
			rf.mu.Unlock()
			reply := AppendEntriesReply{}

			ok := rf.SendAppendEntries(i, &args, &reply)
			if ok == false {
				//DPrintf("SendHeartBeat: SendAppendEntries to %d failed\n", i)
				return
			}

			if reply.Success == true {
				// 如果成功
				*nReplica++
				rf.mu.Lock()
				rf.NextIndex[i] = len(rf.log)
				rf.MatchIndex[i] = len(rf.log) - 1
				DPrintf("[AppendEntries_%s]: Server %d boardCast appendEntry to server %d succeed, Update NextIndex to %v and MatchIndex to %v at term %d ", name, rf.me, i, rf.NextIndex, rf.MatchIndex, CurrentTerm)
				N := len(rf.log) - 1
				if N > rf.CommitIndex &&
					*nReplica >= majority &&
					rf.log[N].Term == CurrentTerm {

					rf.CommitIndex = N
					DPrintf("[AppendEntries_%s]: Server %d update commitIndex to %d at term %d ", name, rf.me, rf.CommitIndex, CurrentTerm)
					rf.mu.Unlock()
					rf.applyCond.Broadcast()
				} else {
					rf.mu.Unlock()
				}
				rf.persist()
				return
			} else {
				// term 过期， back to follower
				rf.mu.Lock()

				if CurrentTerm < reply.Term {
					DPrintf("[AppendEntries_%s]: Server %d term %d expired, back to follower,reset timeout", name, rf.me, CurrentTerm)
					rf.BackToFollower(reply.Term)
					rf.ResetElectionTimer()
					rf.mu.Unlock()
				} else {
					if CurrentTerm > reply.Term {
						DPrintf("[AppendEntries_%s]: Server %d term %d expired, return", name, rf.me, CurrentTerm)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					rf.NextIndex[i] -= 1
					DPrintf("[AppendEntries_%s]: Server %d term %d expired, consistent check failed, retry with nextIndex %d", name, rf.me, CurrentTerm, rf.NextIndex[i])
					goto retry
				}
			}
			//DPrintf("SendHeartBeat:sever %d SendAppendEntries to %d, Term: %d, success %t \n",
			//	rf.me,i, reply.Term, reply.Success)
		}(k, &nReplica)
	}
}
