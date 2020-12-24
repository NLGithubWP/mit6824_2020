package raft

type Entry struct {
	Term	int
	Command	interface{}
}


type AppendEntries struct {
	// leader把它发送给其他server

	Term 			int		// leader 的term
	LeaderId 		int		// leader 的id，每个follower用它来redirect客户的请求

	PrevLogIndex 	int		// 紧邻新日志条目之前的那个日志条目的索引，对于每个follower，发的不一样
	PrevLogTerm 	int		// 紧邻新日志条目之前的那个日志条目的term，对于每个follower，发的不一样

	Entries 		[]*Entry	// 日志内容
	LeaderCommit	int		//	领导者的CommitIndex
}

type AppendEntriesReply struct {
	Term 		int		// 当前follower见过的最新的任期
	Success 	bool	// true 如果follower 获得了entry，并且 PrevLogIndex和PrevLogTerm都符合
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// resets the election timeout so that other servers don't step forward
// as leaders when one has already been elected
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply)  {
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

	if args.Term < rf.CurrentTerm{
		// 1. Reply false if term < currentTerm (§5.1)
		DPrintf("[AppendEntries]: server %d send to %d, but server %d term %d is less than server %d term %d \n",
			args.LeaderId, rf.me, args.LeaderId, args.Term, rf.me, rf.CurrentTerm)
		success = false

	}else{
		if args.Term > rf.CurrentTerm{
			DPrintf("[AppendEntries]: server %d back to follower, args.Term %d, currentTerm: %d \n",
				rf.me, args.Term,rf.CurrentTerm,
			)
			rf.BackToFollower(args.Term)
			rf.ResetElectionTimer()
		}

		//2. Reply false if log doesn’t contain an entry at prevLogIndex
		//	 whose term matches prevLogTerm (§5.3)

		if args.PrevLogIndex <= len(rf.log)-1{
			// 一致性检查失败
			if rf.log[args.PrevLogIndex].Term!=args.PrevLogTerm{
				DPrintf("[AppendEntries]: server %d send to %d, prevLogIndex term  %d is not match %d \n",
					args.LeaderId, rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
				success = false
			}else{
				DPrintf("[AppendEntries]: server %d send to %d, prevLogIndex term  %d is match %d \n",
					args.LeaderId, rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
				//fmt.Println()
				//spew.Printf("[AppendEntries]: server %d send to %d, previous log are: \n%v \n",args.LeaderId, rf.me,  rf.log)

				rf.log = rf.log[:args.PrevLogIndex+1]
				//fmt.Printf("[AppendEntries]: server %d send to %d, length are %d: \n", args.LeaderId, rf.me, args.PrevLogIndex)
				//spew.Printf("[AppendEntries]: server %d send to %d, after log are :\n%v \n",args.LeaderId, rf.me,  rf.log)
				//spew.Printf("[AppendEntries]: server %d send to %d, entries are : \n%v\n", args.LeaderId, rf.me, args.Entries)
				//fmt.Println()
				success = true
			}
		} else{
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

	if success{

		rf.ResetElectionTimer()

		//  4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)

		//	5. If leaderCommit > commitIndex, set commitIndex =
		//	min(leaderCommit, index of last new entry)
		if rf.CommitIndex < args.LeaderCommit{
			if args.LeaderCommit > len(rf.log)-1 {
				rf.CommitIndex = len(rf.log)-1
			}else{
				rf.CommitIndex = args.LeaderCommit
			}
			rf.applyCond.Broadcast()
		}

		reply.Term = rf.CurrentTerm
		reply.Success = success
	}else{
		//DPrintf("AppendEntries: server %d refused hb, and dont reset ResetElection \n", rf.me)
		reply.Term = rf.CurrentTerm
		reply.Success = success
	}
}

func (rf *Raft) SendAppendEntriesToAll(name string){
	nReplica := 1
	majority := len(rf.peers)/2+1

	//fmt.Printf("\n")
	//spew.Printf("[AppendEntries]: server %d SendAppendEntries, current logs are :\n%v \n", rf.me,  rf.log)
	//fmt.Printf("\n")

	CurrentTerm := rf.CurrentTerm
	for i :=0;i<len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		k := i

		go func(i int, nReplica *int){

			retry:
				args := AppendEntries{}
				args.Term = CurrentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.NextIndex[i]-1
				if args.PrevLogIndex < 0{
					DPrintf("[AppendEntries_%s]: Server %d term %d expired, skip retry with nextIndex %d", name,rf.me, CurrentTerm, rf.NextIndex[i])
					return
				}
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = rf.log[rf.NextIndex[i]:]
				args.LeaderCommit = rf.CommitIndex
				reply := AppendEntriesReply{}

				ok:=rf.SendAppendEntries(i, &args, &reply)
				if ok==false{
					//DPrintf("SendHeartBeat: SendAppendEntries to %d failed\n", i)
					return
				}

				if reply.Success==true{
					// 如果成功
					*nReplica ++
					rf.NextIndex[i] = len(rf.log)
					rf.MatchIndex[i] = len(rf.log)-1
					DPrintf("[AppendEntries_%s]: Server %d boardCast appendEntry to server %d succeed, Update NextIndex to %v and MatchIndex to %v at term %d ", name,rf.me,i, rf.NextIndex, rf.MatchIndex,CurrentTerm)
					N := len(rf.log)-1
					if N > rf.CommitIndex &&
						*nReplica >= majority &&
						rf.log[N].Term ==CurrentTerm{

						rf.CommitIndex = N
						DPrintf("[AppendEntries_%s]: Server %d update commitIndex to %d at term %d ", name,rf.me, rf.CommitIndex, CurrentTerm)
						rf.applyCond.Broadcast()
					}
					return
				}else{
					// term 过期， back to follower
					if CurrentTerm < reply.Term{
						DPrintf("[AppendEntries_%s]: Server %d term %d expired, back to follower,reset timeout",name, rf.me, CurrentTerm)
						rf.BackToFollower(reply.Term)
						rf.ResetElectionTimer()
					}else{
						if CurrentTerm >reply.Term{
							DPrintf("[AppendEntries_%s]: Server %d term %d expired, return", name,rf.me, CurrentTerm)
							return
						}
						rf.NextIndex[i] -= 1
						DPrintf("[AppendEntries_%s]: Server %d term %d expired, consistent check failed, retry with nextIndex %d", name,rf.me, CurrentTerm, rf.NextIndex[i])
						goto retry
					}
				}
			//DPrintf("SendHeartBeat:sever %d SendAppendEntries to %d, Term: %d, success %t \n",
			//	rf.me,i, reply.Term, reply.Success)
		}(k, &nReplica)
	}
}