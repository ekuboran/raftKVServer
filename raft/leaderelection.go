package raft

import (
	"math/rand"
	"time"
	"sync"
)


// 进行新一轮的leader投票, rules for candidate: 1
// rf.ticker()里加了函数粒度的锁，其中串行的嵌套函数中就不要加了
func (rf *Raft)leaderElection()  {
	rf.state = CANDIDATE
	rf.currentTerm ++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	DPrintf("[%d]: 开始leader election, term为 %d", rf.me, rf.currentTerm)

	voteCounter := 1			// 已获得的选票数
	myLastEntry := rf.log.lastLog()
	arg := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: myLastEntry.Index,
		LastLogTerm: myLastEntry.Term,
	}
	var becomeLeader sync.Once			// Once是只执行一次动作的对象，确保某个函数只会执行一次
	
	for peerId, _ := range rf.peers {
		if peerId != rf.me {
			go rf.candidateRequestVote(peerId, arg, &voteCounter, &becomeLeader)
		}
	}
}


// 重置选举计时器，设置为300-600ms的随机时间
func (rf *Raft)resetElectionTimer()  {
	currentTime := time.Now()
	rf.electionTimeout = currentTime.Add(time.Duration(rand.Intn(300)+300) * time.Millisecond)
}


// 设置新的任期号，转换为follower。即rules for all servers: 2（两种情况：1.收到leader的心跳由candidate转为follower；2.发现更高的term）
func (rf *Raft)setNewTerm(term int)  {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.persist()
	DPrintf("[%d]: 转换成follower, 设置term为 %d\n", rf.me, rf.currentTerm)
}