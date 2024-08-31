package raft

// 这是raft必须向服务(或测试人员)公开的API的大纲。有关这些函数的详细信息，请参阅下面的注释。
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

import (
//	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"bytes"
	"log"
	"math"
)

const (
	LEADER = "LEADER"
	FOLLOWER = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
)

// 当每个Raft peer意识到连续的日志条目被提交时，
// peer应该通过传递给Make()的applyCh向同一服务器上的服务(或测试器)发送ApplyMsg。
// 将CommandValid设置为true，表示ApplyMsg包含新提交的log entry。
// 在2D部分，你会想要在applyCh上发送其他类型的消息(例如，snapshots);此时，您可以向ApplyMsg添加字段，但请将CommandValid设置为false用于其他用途。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex         	// Lock to protect shared access to this peer's state 保护对这个peer的state的共享访问的锁
	peers     []*labrpc.ClientEnd	// RPC end points of all peers 						  所有peer的RPC指针(client end就是客户端的意思)
	persister *Persister         	// Object to hold this peer's persisted state		  保存此peer持久状态的对象
	me        int                	// this peer's index into peers[]。					   这个peer在peers[]数组中的索引
	dead      int32              	// set by Kill()									  由Kill()设置	

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.
	state string					// 该raft server的状态：LEADER、FOLLOWER、CANDIDATE
	heartBeat time.Duration			// 心跳间隔时间，time.Duration用于表示时间间隔或持续时间的类型.它是一个基于 int64 的整数类型，单位是纳秒。
	electionTimeout time.Time		// 选举计时器 
	
	// persistent state on all servers
	currentTerm int				  	// 当前任期
	votedFor int 			  		// 当前任期内收到选票的candidateId （没有则为-1)
	log Log							// 日志条目

	// volatile state on all servers
	commitIndex int					// 已知被提交的最高的日志条目的索引(初始为0，单调增)
	lastApplied int					// 已经被应用到状态机的最高日志条目的索引(初始为0，单调增)

	
	// volatile state on leaders 
	nextIndex []int					// 对每个server而言，发送到该server的下一个log entry的索引(初始化为leader的最后一个index+1)
	matchIndex []int				// 对每个server而言，已知的已经复制到该server的最高log entry的索引(初始为0，单增)

	
	applyCond *sync.Cond			//
	applyCh chan ApplyMsg			// 发送ApplyMsg的管道

	snapshotData []byte				// 最近快照的数据
}

// return currentTerm and whether this server believes it is the leader.
// 返回currentTerm以及这个服务器认为自己是否是leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == LEADER
	// Your code here (2A).
	return term, isleader
}

// 将Raft的持久状态保存到稳定的存储中，以便在崩溃和重启后检索。关于什么应该是持久化的描述，请参见论文的图2。
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("[%d]: 保存当前状态", rf.me)
}


// 恢复以前保存的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var logs Log
	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil  || d.Decode(&logs) != nil {
		log.Fatal("读取persist失败\n")
	} else {
	  rf.currentTerm = term
	  rf.votedFor = voteFor
	  rf.log = logs
	}
}

// 将Raft的持久状态和快照保存到稳定的存储中
func (rf *Raft) persistStateAndSnapshot()  {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshotData)
	DPrintf("[%d]: 保存当前状态和快照", rf.me)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 服务需要切换到快照。只有当Raft没有更多的最新信息时才这样做，因为它在applyCh上传递快照。
// 这里的raft server是follower
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d](term %d): 正在安装Condsnapshot:lastIncludedIndex=%d,lastIncludedTerm=%d;commitIndex=%d\n", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm, rf.commitIndex)

	// 如果是旧快照，返回false
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("[%d](term %d):Condsnapshot已过期, 安装失败:lastIncludedIndex=[%d],lastIncludedTerm=[%d]\n", rf.me,  rf.currentTerm, lastIncludedIndex, lastIncludedTerm)
		return false
	}

	if lastIncludedIndex > rf.log.lastLog().Index {
		// 如果快照的lastIncludedIndex比当前日志的最大Index还大，就清空log(保留0位置的占位点)
		tmpEntries := rf.log.trim(rf.log.len())
		rf.log.Entries = append([]Entry{{-1, 0, 0}}, tmpEntries...)
	} else {
		// 裁剪日志
		realIndex := rf.log.searchRealIndex(lastIncludedIndex)
		tmpEntries := rf.log.trim(realIndex+1)
		rf.log.Entries = append([]Entry{{-1, 0, 0}}, tmpEntries...)		// 仍要保持Raft日志索引0位置的占位点

	}
	rf.log.Entries[0].Index = lastIncludedIndex						// 将占位点的索引改为lastIncludedIndex
	rf.log.Entries[0].Term = lastIncludedTerm						// 将占位点的任期改为lastIncludedTerm
	rf.snapshotData = snapshot

	// 更新commitIndex和lastAppliedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	rf.persistStateAndSnapshot()
	for _, entry := range rf.log.Entries {
		DPrintf("entry.Index:%d", entry.Index)
	}
	DPrintf("[%d]:(term %d): 安装Condsnapshot成功。 lastIncludedIndex=%d,lastIncludedTerm=%d;commitIndex=%d; lastLogIndex=%d\n", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm, rf.commitIndex, rf.log.lastLog().Index)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 该服务表示，它已经创建了一个快照。快照包括索引处和索引之前的所有信息。这意味着相应的Raft peer不再需要该索引之前（包括该索引）的日志。Raft现在应该尽可能地修剪它的日志。
// 这里的raft server 可以是leader也可以是follower
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// index越界处理
	if index > rf.log.lastLog().Index || index < rf.log.Entries[0].Index || index > rf.commitIndex {
		return 
	}

	// 获取该index的日志条目对应在Entries[]数组中的真实索引
	realIndex := rf.log.searchRealIndex(index)
	DPrintf("index:%d, realIndex:%d", index, realIndex)
	lastIncludedIndex, lastIncludedTerm := rf.log.Entries[realIndex].Index, rf.log.Entries[realIndex].Term
	DPrintf("[%d]:(term %d): 安装snapshot:lastIncludedIndex=%d,lastIncludedTerm=%d;commitIndex=%d\n", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm, rf.commitIndex)

	// 裁剪Raft日志
	tmpEntries := rf.log.trim(realIndex+1)
	rf.log.Entries = append([]Entry{{-1, 0, 0}}, tmpEntries...)		// 仍要保持Raft日志索引0位置的占位点
	rf.log.Entries[0].Index = lastIncludedIndex						// 将占位点的索引改为lastIncludedIndex
	rf.log.Entries[0].Term = lastIncludedTerm						// 将占位点的任期改为lastIncludedTerm

	rf.snapshotData = snapshot
	rf.persistStateAndSnapshot()
	DPrintf("[%d]:(term %d): 安装snapshot成功。 lastIncludedIndex=%d,lastIncludedTerm=%d;commitIndex=%d; lastLogIndex=%d\n", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm, rf.commitIndex, rf.log.lastLog().Index)
}



// 使用Raft协议的服务（例如k/v 服务器）想要启动在 Raft 日志中追加下一条命令的一致性协议。
// (即向Raft集群提交新的日志条目，用于将新的命令或请求添加到Raft日志中)
// 如果这个服务器不是leader，返回false。否则开启一致性协议(start agreement)并立即返回。
// 不能保证这个命令会被提交到Raft日志中，因为leader可能会失败或输掉选举。
// 即使Raft实例已经被kill，这个函数也应该优雅地返回。
// 第一个返回值是命令在提交时出现的索引。第二个返回值是current term。如果此服务器认为它是leader，则第三个返回值为true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, rf.currentTerm, false
	}

	index := rf.log.lastLog().Index + 1
	term := rf.currentTerm

	newEntry := Entry{				// 将客户端命令构造为一个entry
		Command: command,
		Term: term,
		Index: index,
	}

	// rules for leaders: 2 (前半部分)
	rf.log.append(newEntry)			// 在leader的日志中添加该entry
	rf.persist()
	DPrintf("[%d]: (term %d) Start agreement\n", rf.me, rf.currentTerm)
	rf.appendEntries(false)

	return index, term, true
}

// the tester不会在每次测试后停止由Raft创建的go程，但它会调用Kill()方法。
// 你的代码可以使用killed()来检查Kill()是否被调用。atomic的使用避免了对锁的需要。
// 问题是长时间运行的go程使用内存并可能消耗CPU时间，可能导致以后的测试失败并生成令人困惑的调试输出。
// 任何具有长时间循环的go程都应该调用killed()来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker()  {
	for !rf.killed(){				// 只要该raft server没死，就一直循环
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		// rules for leaders 1
		if rf.state == LEADER {		// 当前状态是leader就发心跳包
			rf.appendEntries(true)
		}
		// rules for followers 2
		if time.Now().After(rf.electionTimeout) {		// 选举超时，就发起新一轮选举
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

// 将已提交的日志应用到状态机(实际就是告诉应用层这些命令已经提交了，可以应用了)
// 当目前没有可以应用的日志的时候，就在cond上等待，所有更新commitIndex的代码后面都会唤醒在该cond上等待的协程
func (rf *Raft) applier()  {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		
		firstIndex, commitIndex, lastApplied := rf.log.Entries[0].Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.log.Entries[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("[%d]: applies entries %v-%v in term %v\n", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(commitIndex)))
		rf.mu.Unlock()
	}
}
// func (rf *Raft) applier() {
// 	for !rf.killed() {
// 		// rules for all servers: 1
// 		rf.mu.Lock()
// 		if rf.commitIndex > rf.lastApplied && rf.log.lastLog().Index > rf.lastApplied {
// 			rf.lastApplied ++
// 			DPrintf("[%d](term %d): 我的commitIndex=%d,我的lastApplied变成:%d", rf.me, rf.currentTerm, rf.commitIndex,rf.lastApplied)
// 			applyMsg := ApplyMsg{
// 				CommandValid: true,
// 				Command: rf.log.getEntry(rf.lastApplied).Command,
// 				CommandIndex: rf.lastApplied,
// 				CommandTerm: rf.currentTerm,
// 			}
// 			// 发送到管道上即已经提交了，正在应用到状态机上
// 			rf.mu.Unlock()						// 先解锁（Unlock）再上锁（Lock）的目的是避免在发送消息到 applyCh 时持有锁。
// 			rf.applyCh <- applyMsg				// 这是因为在发送消息到 applyCh 之后，可能会触发 applyCh 的接收端（在其他 goroutine 中）执行一些耗时的操作，
// 			rf.mu.Lock()						// 如果在发送消息期间仍持有锁，那么其他 goroutine 就无法进入临界区。
// 		} else {
// 			DPrintf("[%d]: rf.applyCond.Wait()\n", rf.me)
// 			rf.applyCond.Wait()	
// 		}
// 		rf.mu.Unlock()
// 	}
// }
func (rf *Raft) apply() {			// 别在apply()函数内加锁，因为使用apply()函数的顺序执行流内已经加了锁了
	rf.applyCond.Broadcast()
	DPrintf("[%d]: rf.applyCond.Broadcast()\n", rf.me)
}

// the service or tester 想要创建一个Raft peer服务器。所有Raft服务器的端口(包括这个)都在peer[]中。
// 该服务器的端口是peers[me]。所有服务器的peers[]数组都有相同的顺序。
// persister是此服务器保存其持久状态的地方，并且初始化时还保存最近保存的状态(如果有的话)。
// applyCh是一个管道，the service or tester希望Raft在该管道上发送ApplyMsg消息。Make()必须快速返回，因此它应该为任何长时间运行的工作启动go程。
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	
	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.heartBeat = 100 * time.Millisecond	// 设置心跳间隔为100ms
	rf.votedFor = -1						// 初始时没有收到选票的candidate，因此设为-1(若是string类型设为空串)
	rf.resetElectionTimer()					// 重置选举计时器

	rf.log = makeEmptyLog()
	rf.log.append(Entry{-1, 0, 0})			// 添加一个无效的日志条目，即占位点，让日志条目的索引从1开始
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	rf.snapshotData = persister.snapshot
	rf.lastApplied = rf.log.Entries[0].Index
	rf.commitIndex = rf.log.Entries[0].Index
	
	go rf.ticker()
	go rf.applier()
	DPrintf("启动一个Raft server")
	return rf
}


func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}