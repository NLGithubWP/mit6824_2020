package raft

import (
	"time"
)

func (rf *Raft) BackToFollower(term int){
	//DPrintf("[CandidateAction] : %d  Convert to follower \n", rf.me)
	rf.CurrentTerm = term
	rf.State = Follower
	// goes to un-voted stats
	rf.VotedFor = -1
}

func  (rf *Raft) FollowerAction (){
	go func(){
		for {
			_, isLeader := rf.GetState()
			if isLeader{
				rf.mu.Lock()
				DPrintf("FollowerAction: server %d become leader at term %d, blocking...\n", rf.me,  rf.CurrentTerm)
				rf.noLeaderCond.Wait()
				rf.mu.Unlock()

			}else{
				rf.mu.Lock()

				elapseTime := time.Now().UnixNano() - rf.latestHeardTime
				//DPrintf("FollowerAction: server %d  curreut time reaches %d \n",
				//	rf.me, int(elapseTime/int64(time.Millisecond)))
				if int(elapseTime/int64(time.Millisecond)) >= rf.electionTimeout{

					go rf.CandidateAction()
				}
				rf.mu.Unlock()

			}
			time.Sleep(time.Millisecond*10)
		}
	}()
}
