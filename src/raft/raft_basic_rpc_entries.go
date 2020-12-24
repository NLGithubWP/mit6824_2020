package raft

import (
	"time"
)



type AppendEntries struct {
	// leader把它发送给其他server

	Term 			int		// leader 的term
	LeaderId 		int		// leader 的id，每个follower用它来redirect客户的请求

	PrevLogIndex 	int		// 紧邻新日志条目之前的那个日志条目的索引，对于每个follower，发的不一样
	PrevLogTerm 	int		// 紧邻新日志条目之前的那个日志条目的term，对于每个follower，发的不一样

	Entries 		[]*Entry	// 日志内容
	LeaderCommit	int		//	领导者的CommitIndex
}

type Entry struct {
	Term	int
	Command	interface{}
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

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.CurrentTerm{
		//DPrintf("SendHeartBeat: server %d send to %d, but server %d term %d is less than server %d term %d \n",
		//	args.LeaderId, rf.CurrentTerm, args.LeaderId, reply.Term, rf.me, rf.CurrentTerm)

		success = false

		//2. Reply false if log doesn’t contain an entry at prevLogIndex
		//	 whose term matches prevLogTerm (§5.3)
	}else{

		if args.Term > rf.CurrentTerm{
			rf.BackToFollower(args.Term)

		}

		rf.CurrentTerm = args.Term

		if args.PrevLogIndex <= len(rf.log)-1 && rf.log[args.PrevLogIndex].Term!=args.PrevLogTerm{

			DPrintf("SendHeartBeat: prevLogIndex term  %d is not match %d \n",
				rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

			success = false

		}else{
			/*
				 3. If an existing entry conflicts with a new one (same index
					   but different terms), delete the existing entry and all that
					   follow it (§5.3)
			*/
			for i, v := range args.Entries{
				if rf.log[args.PrevLogIndex+1+i].Term != v.Term{

					rf.log = rf.log[:args.PrevLogIndex+1+i]
					success = true
					break
				}
			}

			if len(args.Entries) == 0{
				//DPrintf("AppendEntries: server %d got hb \n", rf.me)
				success = true
			}
		}
	}

	if success{

		//DPrintf("AppendEntries: server %d reset ResetElection \n", rf.me)
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
		}

		reply.Term = rf.CurrentTerm
		reply.Success = success
	}else{
		//DPrintf("AppendEntries: server %d refused hb, and dont reset ResetElection \n", rf.me)
		reply.Term = rf.CurrentTerm
		reply.Success = success
	}
}


func (rf *Raft) SendHeartBeat(){
	// update lastSendTime
	rf.lastSendTime = time.Now().UnixNano()
	nReplica := 1
	rf.SendAppendEntriesToAll(nReplica)
}

func (rf *Raft) SendAppendEntriesToAll(nReplica int){

	for i :=0;i<len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		k := i

		go func(i int){

			args := new(AppendEntries)
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.NextIndex[i]-1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = make([]*Entry, 0)
			args.LeaderCommit = rf.CommitIndex

			reply := new(AppendEntriesReply)

			ok:=rf.SendAppendEntries(i, args, reply)
			if ok==false{
				//DPrintf("SendHeartBeat: SendAppendEntries to %d failed\n", i)
				return
			}
			//DPrintf("SendHeartBeat:sever %d SendAppendEntries to %d, Term: %d, suceess %t \n",
			//	rf.me,i, reply.Term, reply.Success)
			// todo add another thread to procecss the logs, this should be only for heartbeat
		}(k)
	}
}