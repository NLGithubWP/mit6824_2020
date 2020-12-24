package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term 			int		//候选人term
	CandidateId	 	int		//候选人ID
	LastLogIndex	int		//最新log entry 的index
	LastLogTerm		int		//最新log entry 的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term 			int
	VoteGranted 	bool	// true means candidate receive the vote

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[RequestVote]: %d Getting RequestVote requests %v, %v ! \n", rf.me, args, reply)
	// Your code here (2A, 2B).

	/*
		Receiver implementation:
		1. Reply false if term < currentTerm (§5.1)
		2. If votedFor is null or candidateId, and candidate’s log is at
		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	*/

	// 1. Reply false if term < currentTerm
	var isVote bool

	if args.Term < rf.CurrentTerm{
		isVote = false
		DPrintf("[RequestVote]: %d,Term %d,is larger than candidate's term %d, refuse vote! \n",
			rf.me, rf.CurrentTerm, args.Term)
	}else{

		//  If RPC request or response contains term T > currentTerm:
		//  set currentTerm = T, convert to follower (§5.1)
		if rf.CurrentTerm < args.Term{
			rf.BackToFollower(args.Term)
		}else{
			DPrintf("[RequestVote]: %d,Term %d,candidate's term %d, not back to follower \n",
				rf.me , rf.CurrentTerm, args.Term)
		}

		/*
			2. Raft determines which of two logs is more up-to-date
			by comparing the index and term of the last entries in the
			logs. If the logs have last entries with different terms, then
			the log with the later term is more up-to-date. If the logs
			end with the same term, then whichever log is longer is
			more up-to-date.
		*/

		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId{
			// 比较最后一位的term，如果不相同，term大的最新
			if args.LastLogTerm !=  rf.log[len(rf.log)-1].Term{

				// 如果 args的term新
				if args.LastLogTerm > rf.log[len(rf.log)-1].Term{

					DPrintf("[RequestVote]: %d vote for %d, Term %d, candidate's term %d \n",
						rf.me ,args.CandidateId, rf.CurrentTerm, args.Term)
					isVote = true
				}else{
					// 如果 我的term新，拒绝
					DPrintf("[RequestVote] : %d No vote for %d, Term %d is larger than candidate's term %d \n",
						rf.me ,args.CandidateId, rf.CurrentTerm, args.Term)

					isVote = false
				}
			}else{
				// if term is the same, compare which one is longer
				// 最后一位的term相同，长度最长的最新
				if len(rf.log) > args.LastLogIndex+1{
					DPrintf("[RequestVote] : %d No vote for %d, im longer %d, candidate's LastLogIndex %d \n",
						rf.me ,args.CandidateId, len(rf.log), args.LastLogIndex)

					isVote = false
				}else{

					DPrintf("[RequestVote] : %d vote for %d, im shorter %d, candidate's LastLogIndex %d \n",
						rf.me ,args.CandidateId, len(rf.log), args.LastLogIndex)
					isVote = true
				}
			}

		}else{
			isVote = false
			DPrintf("[RequestVote]: %d refused vote for %d, Term %d , candidate's term %d , because current " +
				"rf.VotedFor== %d \n", rf.me ,args.CandidateId, rf.CurrentTerm, args.Term, rf.VotedFor)
		}
	}

	if isVote{
		rf.State = Follower

		rf.VotedFor = args.CandidateId

		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
	}else{
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
}

