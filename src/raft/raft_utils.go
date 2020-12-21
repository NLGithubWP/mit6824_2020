package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func (rf *Raft) IsTermExpired(term int) bool{
	rf.mu.Lock()
	if rf.CurrentTerm != term {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	return true
}


func (rf *Raft) ResetElectionTimer(){
	rand.Seed(time.Now().UnixNano())
	// expand the timeout, locking is time consuming, sometimes, different server only have
	// 2ms differences.
	rf.electionTimeout = rf.heartbeatTimeout*5 + rand.Intn(300-150)
	rf.latestHeardTime = time.Now().UnixNano()

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.State == Leader{
		isleader = true
	}else{
		isleader = false
	}
	rf.mu.Unlock()
	//DPrintf("GetState: server %d is at term %d \n", rf.me, term)

	return term, isleader
}


//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


