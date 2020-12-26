package raft

import (
	"sync"
)

func (rf *Raft) CandidateAction() {

	rf.mu.Lock()

	rf.State = Candidate

	// 1. Increment currentTerm
	rf.CurrentTerm += 1

	// 2. Vote for self
	rf.VotedFor = rf.me
	// vote: how many vote got
	vote := 1
	rf.persist()
	rf.mu.Unlock()
	// 3. Reset election timer
	rf.ResetElectionTimer()

	DPrintf("[CandidateAction] : server %d election Timeout, begin selection, currently term is %d and got vote "+
		"%d, there are %d peers: \n", rf.me, rf.CurrentTerm, vote, len(rf.peers))
	DPrintf("[CandidateAction] : server %d reset timeout to %d  \n",
		rf.me, rf.electionTimeout)

	// 4. Send requestVote RPCS to all servers
	go rf.boardCaseRequestVote(&vote)
}

func (rf *Raft) boardCaseRequestVote(vote *int) {
	g := sync.WaitGroup{}

	args := new(RequestVoteArgs)
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[args.LastLogIndex].Term

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		g.Add(1)
		k := i
		DPrintf("[CandidateAction] : %d send to %d Term %d \n", rf.me, k, rf.CurrentTerm)
		go rf.requestVote(vote, &g, args, k)
	}
	g.Wait()
}

func (rf *Raft) requestVote(vote *int, g *sync.WaitGroup, args *RequestVoteArgs, k int) {
	defer g.Done()
	reply := new(RequestVoteReply)

	ok := rf.sendRequestVote(k, args, reply)

	if ok == false {
		DPrintf("[CandidateAction] : %d sendRequestVote to %d failed \n", rf.me, k)
		return
	}

	// receiver implementation::
	/*
		1. Reply false if term < currentTerm (§5.1)
		2. If votedFor is null or candidateId, and candidate’s log is at
		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	*/
	if reply.VoteGranted == false {
		rf.mu.Lock()
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if reply.Term > rf.CurrentTerm {
			DPrintf("[CandidateAction] : %d sendRequestVote to %d succeed, but %d dont vote since: reply term %d is bigger %d, back to follower \n",
				rf.me, k, k, reply.Term, rf.CurrentTerm)
			rf.BackToFollower(reply.Term)
			rf.mu.Unlock()
			return
		} else {
			DPrintf("[CandidateAction] : %d sendRequestVote to %d succeed, but %d dont vote since: the log may not up-to-data compared with server %d \n",
				rf.me, k, k, k)
			rf.mu.Unlock()
		}

	} else if reply.VoteGranted == true {

		*vote += 1
		DPrintf("[CandidateAction] : %d sendRequestVote to %d succeed, got %d vote at term %d \n",
			rf.me, k, *vote, rf.CurrentTerm)
		if rf.State != Leader {
			if *vote >= len(rf.peers)/2+1 {
				DPrintf("[CandidateAction] : <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< server %d ,become leader at "+
					"term %d , it got %d vote >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n", rf.me, rf.CurrentTerm, *vote)
				rf.State = Leader
				// lock should not include Broadcast
				rf.leaderCond.Broadcast()
				rf.persist()
			}
		}
	}
}
