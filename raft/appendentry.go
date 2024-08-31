package raft

import (
	"math"
)

// AppendEntries RPC 参数结构.
type AppendEntriesArgs struct {
	Term int				// leader的任期
	LeaderId int			// leader的id,由此follower可以将客户端的请求重定向给leader
	PreLogIndex int			// 紧邻新条目之前的日志条目的索引
	PreLogTerm int			// 紧邻新条目之前的日志条目的任期
	Entries []Entry			// 需要被保存的日志条目(用于心跳时为空；为了效率可能一次性发多个)
	LeaderCommit int		// leader的commitIndex
}


// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term int				// currentTerm, 用于leader更新自己的任期号
	Success bool			// true表示follower所含的entry和prevLogIndex以及prevLogTerm匹配上了

	// 以下属性用于roll back quickly
	Conflict bool			// 
	XTerm int				// follower在冲突条目中对应的term(也可能没有冲突条目，仅仅只是太短了，这种情况就设为-1)
	XIndex int				// follower在XTerm条目下的第一个索引
	XLen int				// 对应没有冲突条目，只是太短时，follower的log Entry长度
}


// 给每个follower发送RPC心跳包或AERPC
// 这里不能像candidate参选时设置voteCounter那样设置apeendSuccessCounter，因为AERPC既包括了append entry的AERPC，也包括心跳。
// 即不要通过AERPC的reply来判断是否追加日志成功，而是通过matchIndex判断
func (rf *Raft) appendEntries(isHeartbeat bool)  {
	lastEntry := rf.log.lastLog()				// 取leader的最后一个entry（就是新的entry）

	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			rf.resetElectionTimer()				// 这里需要重置选举计时器，确保在 leader 活跃的情况下，其选举计时器不会超时而引发新一轮选举
			continue
		}

		// rules for leader: 3 (前半部分)
		if lastEntry.Index >= rf.nextIndex[peerId] || isHeartbeat {  	// 即如果有新的日志条目或是心跳 
			nextIndex := rf.nextIndex[peerId]
			if nextIndex < 1 {					// 因为AERPC不成功的时候会对nextIndex递减，所以可能出现<1的情况(注：日志的第一个索引是1)
				nextIndex = 1
			}

			if lastEntry.Index+1 < nextIndex {
				nextIndex = lastEntry.Index
			}
			if nextIndex <= rf.log.Entries[0].Index {
				// 如果要传的日志条目在快照中，就发送InstallSnapshot RPC
				args := InstallSnapshotArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					LastIncludedIndex: rf.log.Entries[0].Index,
					LastIncludedTerm: rf.log.Entries[0].Term,
					Data: rf.snapshotData,
				}
				go rf.leaderInstallSnapshot(peerId, &args)
			}else {
				// 如果要传递日志条目在日志中，就发送AppendEntries RPC
				DPrintf("[%d]:我存的[%d]的nextIndex=%d, 我的头index=%d, 我的尾index=%d", rf.me, peerId, nextIndex, rf.log.Entries[0].Index, rf.log.lastLog().Index)
				preEntry := rf.log.getEntry(nextIndex-1)
				args := AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PreLogIndex: preEntry.Index,
					PreLogTerm: preEntry.Term,
					// Entries: rf.log.Entries[nextIndex:],		不能这么写，go中的切片不会新建一个对象，是在原地址上操作原对象
					Entries: make([]Entry, lastEntry.Index-nextIndex+1),		// 作为心跳的时候，这里是空的，即lastEntry.Index-nextIndex+1=0
					LeaderCommit: rf.commitIndex,
				}
				realNextIndex := rf.log.searchRealIndex(nextIndex)
				copy(args.Entries, rf.log.Entries[realNextIndex:])		// 用copy浅拷贝一个后赋值给args.Entries
				go rf.leaderAppendEntries(peerId, &args)
			}
		}
		
	}
}


// 给某个follower发送RPC心跳包或AppendEntriesRPC，并对reply进行处理
func (rf *Raft) leaderAppendEntries(serverId int, args *AppendEntriesArgs)  {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for all servers 2
	if reply.Term > rf.currentTerm {		// follower比leader的任期大(对应AppendEntries中的receiver implementation 1)，重置leader的任期并贬为follower
		rf.setNewTerm(reply.Term)
		return
	}

	if args.Term == rf.currentTerm {		// 关于这里要进行判断的原因请看student guide中的Term confusion第一段话(即当你收到旧的RPC reply时你应该做什么)
		// rules for leaders: 3 (后半部分)
		if reply.Success {					// 关于matchIndex和nextIndex的设置请看student guide中的Term confusion的第二段话
			match := args.PreLogIndex + len(args.Entries)
			next := match + 1
			rf.matchIndex[serverId] = int(math.Max(float64(rf.matchIndex[serverId]), float64(match)))
			rf.nextIndex[serverId] = int(math.Max(float64(rf.nextIndex[serverId]), float64(next)))
			DPrintf("[%v]: [%v] append success, nextIndex:%v matchIndex:%v\n", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {			// fast backup优化
			DPrintf("[%d]: Conflict from [%d]", rf.me, serverId)
			if reply.XTerm == -1{			// 对应follower的log entry太短的情况
				rf.nextIndex[serverId] = reply.XLen
			} else {						// 对应follower的entry和leader的entry有冲突的两种情况（一是leader有冲突的XTerm，另一种是leader没有冲突的XTerm）
				lastXTermEntry := -1		// leader中属于XTerm的条目的最后一个索引（如果有）。用这个来判断具体是哪种情况
				for i:=rf.log.Entries[0].Index; i <= rf.log.lastLog().Index; i++ {
					if rf.log.getEntry(i).Term == reply.XTerm {
						lastXTermEntry = i
					} else if rf.log.getEntry(i).Term > reply.XTerm {
						break
					}
				}
				DPrintf("[%v]: lastXTermEntry %v\n", rf.me, lastXTermEntry)
				if lastXTermEntry > 0 {
					rf.nextIndex[serverId] = lastXTermEntry
				} else {
					rf.nextIndex[serverId] = reply.XIndex
				}
			}
			DPrintf("[%d]: leader的nextIndex[%d]修改为: %v", rf.me, serverId, rf.nextIndex[serverId])
		} //else if rf.nextIndex[serverId] >1 {
		// 	rf.nextIndex[serverId] --
		// }
		
		// rules for leaders: 4
		rf.leaderCommitRule()
	}
	
}

// leader的提交规则，rules for leader: 4
func (rf *Raft) leaderCommitRule()  {
	if rf.state != LEADER {
		return
	}

	for n := rf.commitIndex+1; n <= rf.log.lastLog().Index; n ++ {
		realn := rf.log.searchRealIndex(n)
		if rf.log.Entries[realn].Term != rf.currentTerm {
			continue
		}
		counter := 1
		for peerId, _ := range rf.peers {
			if peerId != rf.me && rf.matchIndex[peerId] >= n {
				counter ++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n					// 这里leader修改自己的commitIndex就相当于隐式地提交了日志
				DPrintf("[%d]leader尝试提交index %d\n", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}



// AppendEntries RPC handler. leader调用，candidate和follower接收
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]:(term %d)follower收到来自[%d]的AERPC, preLogIndex=%d, preLogTerm=%d\n", rf.me, rf.currentTerm, args.LeaderId, args.PreLogIndex, args.PreLogTerm)

	// rules for all servers 2
	if args.Term > rf.currentTerm {			// leader比follower的任期大，重置follower的任期
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.setNewTerm(args.Term)
		return								// 这里重置term后要return是因为收到args的term大于自己的term是非正常执行的，因为作为leader发给follower的AERPC，其term应该是相同的
	}

	// receiver implementation 1
	if args.Term < rf.currentTerm {			// leader比follower的任期小
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 至此正式接收到当前leader的AERPC（收到心跳，重置选举计时器），但并不是接受了append
	rf.resetElectionTimer()

	// candidate rule 3
    if rf.state == CANDIDATE {
        rf.state = FOLLOWER
    }

	// 因为心跳和追加日志都要发AERPC，则可能出现这种情况：
	// 首先发出去心跳的AERPC，这时的nextIndex[f]比较小，因此参数中的preLogIndex也比较小，
	// 然后有新命令来了,发送正常的AERPC，得到追加成功的响应后日志变大了
	// 然后当前follower又对日志做了快照，导致迟到的心跳AERPC中args.PreLogIndex比lastLogIndex还要小
	if args.PreLogIndex < rf.log.Entries[0].Index{
		reply.Term = rf.currentTerm
		return
	}

	// receiver implementation 2
	DPrintf("[%d]:leader的PreLogIndex:%d, 我的头index=%d, 我的尾index=%d",rf.me, args.PreLogIndex, rf.log.Entries[0].Index, rf.log.lastLog().Index)
	if rf.log.lastLog().Index < args.PreLogIndex {
		DPrintf("[%d]: PreLogIndex处没条目\n", rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		return
	}
	if rf.log.getEntry(args.PreLogIndex).Term != args.PreLogTerm {
		DPrintf("[%d]: PreLogIndex处条目term冲突\n", rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.Conflict = true
		xTerm := rf.log.getEntry(args.PreLogIndex).Term
		for xIndex := args.PreLogIndex; xIndex > rf.log.Entries[0].Index; xIndex -- {
			if rf.log.getEntry(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		return
	}
	
	for i, entry := range args.Entries {
		// receiver implementation 3
		index := entry.Index
		if index <= rf.log.lastLog().Index && rf.log.getEntry(index).Term != entry.Term {
			realIndex := rf.log.searchRealIndex(index)
			rf.log.truncate(realIndex)
			rf.persist()
		}
		// receiver implementation 4
		if index > rf.log.lastLog().Index {		// 这里以及上面entry.Index与rf.log.lastLog().Index比较的判断用的很巧妙
			rf.log.append(args.Entries[i:]...)
			rf.persist()
			DPrintf("[%d]: follower append 新条目\n", rf.me)
			break
		}
	}
	
	// reveiver implementation 5 (这个implementation是经过了前4个implementation才正确)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.log.lastLog().Index)))
		rf.apply()
	}

	reply.Term = rf.currentTerm
	reply.Success = true

}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}