package raft

func (rf *Raft) ApplyLogs(){
	// If commitIndex > lastApplied: increment lastApplied,
	// apply log[lastApplied] to state machine (§5.3)
	go func(){
		for i := rf.LastApplied+1; i<=rf.CommitIndex; i++{

			applyMsg :=  ApplyMsg{
				CommandValid: true,
				Command: rf.log[i].Command,
				CommandIndex: i}
			rf.LastApplied += 1
			DPrintf("ApplyLogs: Id %d Term %d State %d apply command %v of index %d and term %d to applyCh\n",
				rf.me, rf.CurrentTerm,rf.State, applyMsg.Command, applyMsg.CommandIndex, rf.log[i].Term)
			rf.applyCh <- applyMsg
		}

		//for{
		//
		//
		//	//// 如果相等，阻塞等待
		//	//if rf.CommitIndex == rf.LastApplied{
		//	//	rf.mu.Lock()
		//	//	rf.applyCond.Wait()
		//	//	rf.mu.Unlock()
		//	//} else {
		//	//
		//	//	// If commitIndex > lastApplied: increment lastApplied,
		//	//	// apply log[lastApplied] to state machine (§5.3)
		//	//
		//	//	for i := rf.LastApplied+1; i<=rf.CommitIndex; i++{
		//	//		rf.mu.Lock()
		//	//
		//	//		applyMsg :=  ApplyMsg{
		//	//			CommandValid: true,
		//	//			Command: rf.log[i].Command,
		//	//			CommandIndex: i}
		//	//
		//	//		rf.LastApplied += 1
		//	//		rf.mu.Unlock()
		//	//		DPrintf("ApplyLogs: Id %d Term %d State %d apply command %v of index %d and term %d to applyCh\n",
		//	//			rf.me, rf.CurrentTerm,rf.State, applyMsg.Command, applyMsg.CommandIndex, rf.log[i].Term)
		//	//		rf.applyCh <- applyMsg
		//	//	}
		//	//
		//	//}
		//}

	}()
}