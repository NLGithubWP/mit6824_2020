package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "../labgob"


const (
	Follower	= 1
	Candidate	= 2
	Leader		= 3
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state

	noLeaderCond      *sync.Cond          //
	leaderCond      *sync.Cond          //
	applyCond		*sync.Cond


	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//  state for all server
	State  		int		// follower ? leader? candidate?

	applyCh		chan ApplyMsg

	// timeout

	electionTimeout		int
	latestHeardTime		int64

	heartbeatTimeout	int
	lastSendTime		int64


	// persistent state for all server
	CurrentTerm		int			//服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增
	VotedFor		int			// 当前任期内，接受到我的投票的那个人的id， 如果我没有投给任何人，为空
	log				[]*Entry	//日志条目;每个条目包含了term+command（第一个索引为1）

	// volatile state for all server
	CommitIndex		int		// 已提交的最高日志条目的index，已提交意味着这个日志同步到大多数sever。
							// An entry is considered committed if it is safe for that entry to be applied to state machines.
							// A log entry is committed once the leader that created
							// the entry has replicated it on a majority of the servers
							// 这个由leader来控制，

	LastApplied		int		// 已应用到状态机的最高日志条目的index。follower根据commitIndex来决定是否更新这个字段

	// volatile state on leaders， 每次选举后，重新init
	NextIndex		[]int	// 长度为n，存着  对于每个服务器，发送到这个服务器的下一条日志条目
	MatchIndex		[]int	// 长度为n，存着  对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); isLeader{
		// index: 将要插入的entry应该在的位置
		index = len(rf.log)
		entry := Entry{term, command}

		rf.log = append(rf.log, &entry)
		rf.lastSendTime = time.Now().UnixNano()
		go rf.SendAppendEntriesToAll("ClientAppend")
	}
	//DPrintf("[Start]: Server %d Term %d Index %d, IsLeader %v\n", rf.me,
	//rf.CurrentTerm, index, isLeader)

	return index, term, isLeader
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCh = applyCh

	// persistent state for all server
	rf.CurrentTerm = 0 // init to 0 on first boot, increase monotonically
	rf.VotedFor=-1	// if null if None
	rf.log = make([]*Entry,1)	// first index is one
	rf.log[0] = &Entry{}

	rf.heartbeatTimeout = 120

	// volatile state for all server
	rf.CommitIndex = 0	// init to 0 ,increase monotonically
	rf.LastApplied = 0	// init to 0 ,increase monotonically

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.noLeaderCond = sync.NewCond(&rf.mu)

	rf.State = Follower
	rf.ResetElectionTimer()

	// all server will do:
	rf.ApplyLogs()

	// Follower will do:
	rf.FollowerAction()

	// Leader will do:
	rf.LeaderAction()

	return rf
}



