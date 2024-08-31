package raft

import (
	"sync"
)

// 示例RequestVote RPC参数结构。
// 字段名必须以大写字母开头!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int				// candidate的term
	CandidateId int			// 请求选票的candidate的id
	LastLogIndex int		// candidate的最后日志条目的索引值
	LastLogTerm int			// candidate的最后日志条目的任期号

}


// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Term int				// currentTerm, 用于candidate更新自己的任期号
	VoteGranted bool		// true表示candidate收到该选票
}


// candidate请求选票，并对reply进行处理。rules for candidate server: 2、3、4
func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int, becomeLeader *sync.Once)  {
	DPrintf("[%d]: in term %d send RequestVote to [%d]\n", rf.me, args.Term, serverId)
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, reply)			// 发送RequestVote RPC给指定server
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for candidate server: 3
	if reply.Term > args.Term {					// peer的比candidate的term大
		DPrintf("[%d]: %d 处于新的term, 更新term, 结束\n", rf.me, serverId)
		rf.setNewTerm(reply.Term)
		return
	}

	if reply.Term < args.Term {					// peer的比candidate的term小
		DPrintf("[%d]: [%d]的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}

	if !reply.VoteGranted {						// peer已经投给别的candidate了
		DPrintf("[%d]: [%d]没有投给自己, 结束\n", rf.me, serverId)
		return
	}

	// 上面对应获取选票失败，下面对应获取选票成功
	*voteCounter ++
	DPrintf("[%d]: 来自[%d]的term一致, 且投给[%d]\n", rf.me, serverId, rf.me)

	
	// 选举成功。每次选举成功后leader中保存的两个状态nextIndex[]和matchIndex[]都要重置
	if *voteCounter > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == CANDIDATE{
		becomeLeader.Do(func ()  {					// becomeLeader这个sync.Once类型变量通过Do()确保candidate到leader的状态转变只发生一次（这些candidateRequestVote是并行的，因此这里只能执行一次）
			DPrintf("[%d]: 选举结束, 当前term为 %d\n", rf.me, rf.currentTerm)
			rf.state = LEADER
			lastEntry := rf.log.lastLog()
			for peerId, _ := range rf.peers {
				rf.nextIndex[peerId] = lastEntry.Index + 1
				rf.matchIndex[peerId] = 0			// 因为Log[]的第一个索引是1，所以matchIndex初始化为0而不是-1
			}
			rf.appendEntries(true)
		})
	}
}


// RequestVote RPC handler. candidate调用，follower接收
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for all servers 2		(这里重置term后不return是因为这里arg的term比自己的term大是正常的，因为candidate参选时会自增term)
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	// receiver implementation 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// receiver implementation 2	(选举限制)
	myLastEntry := rf.log.lastLog()				// 获取自己日志的最后一个条目
	// candidate的日志至少要自己的一样新才给其投票
	upToDate := myLastEntry.Term < args.LastLogTerm || (myLastEntry.Term == args.LastLogTerm && myLastEntry.Index <= args.LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&  upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		DPrintf("[%v](term %v): 投票给[%v]", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}


// 发送RequestVote RPC到服务器的示例代码。
// server是rf.peers[]中目标服务器的索引。
// 在args中期望RPC参数。
// 用RPC reply填充*reply，因此调用者应该传递&reply
// 传递给Call()的args和reply的类型必须与handler函数中声明的参数类型相同(包括它们是否为指针)。
//
// labrpc包模拟了一个有损网络，其中服务器可能不可达，请求和应答可能丢失。
// Call()发送请求并等待reply. 如果reply在超时间隔内到达，Call()返回true; 否则返回false
// 因此Call()可能暂时不会返回。错误的返回可能是由dead server、无法访问的live server、丢失的request或丢失的reply引起的。
//
// 如果服务器端的处理程序函数没有返回，Call()保证返回*except*。  
// Thus there is no need to implement your own timeouts around Call().
//
// 请看../labrpc/labrpc中的注释。去了解更多细节。
//
// 如果在使用RPC进行工作时遇到问题，检查参数字段名是否大写、调用者传递的reply的地址是否有&
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}