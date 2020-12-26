package raft

import "time"

func (rf *Raft) ApplyLogs() {
	// If commitIndex > lastApplied: increment lastApplied,
	// apply log[lastApplied] to state machine (§5.3)
	go func() {

		for {
			rf.mu.Lock()
			commitIndex := rf.CommitIndex
			lastApplied := rf.LastApplied
			rf.mu.Unlock()

			// 如果相等，阻塞等待
			if commitIndex == lastApplied {
				rf.mu.Lock()
				rf.applyCond.Wait()
				rf.mu.Unlock()
			} else {

				// If commitIndex > lastApplied: increment lastApplied,
				// apply log[lastApplied] to state machine (§5.3)
				DPrintf("[ApplyLogs]: Server %d ApplyLogs Awake! checking lastApplied: %d CommitIndex: %d\n",
					rf.me, lastApplied, commitIndex)

				for i := lastApplied + 1; i <= commitIndex; i++ {
					rf.mu.Lock()
					if i >= len(rf.log) {
						rf.mu.Unlock()
						continue
					}

					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i}

					rf.LastApplied += 1
					DPrintf("[ApplyLogs]: Server %d Term%d State %d apply command %v of index %d and term %d to applyCh\n",
						rf.me, rf.CurrentTerm, rf.State, rf.log[i].Command, applyMsg.CommandIndex, rf.log[i].Term)
					rf.mu.Unlock()

					rf.applyCh <- applyMsg
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}()
}
