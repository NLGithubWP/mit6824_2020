package raft

import (
	"time"
)

func  (rf *Raft) LeaderAction (){
	/*
		Upon election: send initial empty AppendEntries RPCs
		(heartbeat) to each server; repeat during idle periods to
		prevent election timeouts (§5.2)
		• If command received from client: append entry to local log,
		respond after entry applied to state machine (§5.3)
		• If last log index ≥ nextIndex for a follower: send
		AppendEntries RPC with log entries starting at nextIndex
		• If successful: update nextIndex and matchIndex for
		follower (§5.3)
		• If AppendEntries fails because of log inconsistency:
		decrement nextIndex and retry (§5.3)
		• If there exists an N such that N > commitIndex, a majority
		of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		set commitIndex = N (§5.3, §5.4).

	 */
	go func(){

		for{
			if term, isLeader := rf.GetState(); isLeader == false {
				// if not leader, wait
				rf.mu.Lock()
				DPrintf("[LeaderAction] %d: Not leader, wait here, term is %d \n", rf.me, term)
				rf.leaderCond.Wait()
				rf.mu.Unlock()

				// initialized  NextIndex and MatchIndex according to current logs, after election
				rf.NextIndex = make([]int, len(rf.peers))
				rf.MatchIndex = make([]int,len(rf.peers))
				for i:=0;i<len(rf.peers);i++{
					//(initialized to leader last log index + 1)
					rf.NextIndex[i] = len(rf.log)-1+1
					//(initialized to 0, increases monotonically)
					rf.MatchIndex[i] = 0
				}
				// send hb
				go rf.SendHeartBeat()
				DPrintf("[LeaderAction]: server %d Awake from sleep, now Im leader!\n", rf.me)
			} else {
				rf.mu.Lock()
				// if it's leader, send heartBeat to all
				if int((time.Now().UnixNano() - rf.lastSendTime)/int64(time.Millisecond)) >= rf.heartbeatTimeout{
					//DPrintf("LeaderAction leader %d send hb at term %d \n", rf.me, term)
					go rf.SendHeartBeat()
				}
				rf.mu.Unlock()
			}
		}
	}()
}