package raft

import "time"

func (rf *Raft) ApplyLogs(){
	// If commitIndex > lastApplied: increment lastApplied,
	// apply log[lastApplied] to state machine (§5.3)
	go func(){

		for{
			// 如果相等，阻塞等待
			if rf.CommitIndex == rf.LastApplied{
				rf.mu.Lock()
				rf.applyCond.Wait()
				rf.mu.Unlock()
			} else {

				// If commitIndex > lastApplied: increment lastApplied,
				// apply log[lastApplied] to state machine (§5.3)
				DPrintf("[ApplyLogs]: Server %d ApplyLogs Awake! checking lastApplied: %d CommitIndex: %d\n",
					rf.me, rf.LastApplied, rf.CommitIndex)

				for i := rf.LastApplied+1; i<=rf.CommitIndex; i++{

					applyMsg :=  ApplyMsg{
						CommandValid: true,
						Command: rf.log[i].Command,
						CommandIndex: i}

					rf.LastApplied += 1
					DPrintf("[ApplyLogs]: Server %d Term %d State %d apply command %v of index %d and term %d to applyCh\n",
						rf.me, rf.CurrentTerm,rf.State, rf.log[i].Command ,applyMsg.CommandIndex, rf.log[i].Term)
					rf.applyCh <- applyMsg
				}

			}

			time.Sleep(time.Millisecond * 10)
		}

	}()
}