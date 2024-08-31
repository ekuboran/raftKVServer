package raft

// InstallSnapshot RPC 参数结构.
type InstallSnapshotArgs struct {
	Term int					// leader的任期
	LeaderId int				// leader的id
	LastIncludedIndex int		// 快照中包含的最后日志条目的索引
	LastIncludedTerm int		// 快照中包含的最后日志条目的任期
	Data []byte					// 快照数据
}


// InstallSnapshotReply RPC reply structure.
type InstallSnapshotReply struct {
	Term int					// 当前任期号，以便leader更新自己
}

// 给某个follower发送InstallSnapshot RPC，并对reply进行处理
func (rf *Raft) leaderInstallSnapshot(serverId int, args *InstallSnapshotArgs)  {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(serverId, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for all servers 2
	if reply.Term > rf.currentTerm {		// follower比自己的任期大，重置leader的任期并贬为follower
		rf.setNewTerm(reply.Term)
		return
	}

	if args.Term == rf.currentTerm {
		rf.matchIndex[serverId] = args.LastIncludedIndex
		rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
		DPrintf("[%v]: [%v] InstallSnapshot success, nextIndex:%v matchIndex:%v\n", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
	}
}


// leader调用，follower接收
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]:(term %d)follower 收到来自[%d]的ISRPC, LastIncludedIndex=%d, LastIncludedTerm=%d\n", rf.me, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)

	reply.Term = rf.currentTerm

	// rules for all servers 2
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	// receiver implementation 1
	if args.Term < rf.currentTerm {
		return
	}

	// 至此正式接收到当前leader的ISRPC（重置选举计时器）
	rf.resetElectionTimer()

	// candidate rule 3
	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}

	// 若快照过期
	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf("[%d]:(term %d)follower收到的来自[%d]的快照已过期:lastLogIndex=[%d],lastLogTerm=[%d]。我的commitIndex=[%d]\n", rf.me,  rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, rf.commitIndex)
		return
	}

	// 没问题则发送到applyCh传到上层service
	applyMsg := ApplyMsg {
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm: args.LastIncludedTerm,
	}
	// 异步传递msg，避免持有锁的时候阻塞，导致死锁
	go func(msg ApplyMsg) {
		rf.applyCh <-msg
	}(applyMsg)

}


func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}