package kvraft

import (
	"bytes"
	"6.824/labgob"
	"6.824/labrpc"
	"log"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)


type Op struct {
	// Your definitions here.
	// 字段名必须以大写字母开头，否则RPC将中断。
	CommandType CommandType		// command类型
	ClientId int64				// client唯一标识
	CommandSeq int				// 该command的序号
	Key string					// 键
	Value string				// 值（如果是Get则为空）
}

//	命令应用完成或是快照应用完成后的通知信息
type ApplyNotifyMsg struct {
	Err Err
	Value string			// Put、Append时为空
}


type CommandContext struct {			// 用于存储对客户端的响应
	CommandSeq int			// 该client目前的commandSeq
	Reply ApplyNotifyMsg	// 对应该commandSeq的响应
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg					// Raft中提交command和snapshot的管道
	dead    int32 								// set by Kill()

	maxraftstate int 							// 日志过大时(超过maxraftstate)进行快照

	// Your definitions here.
	kvDatabase KVDatabase						// KV数据库，即状态机
	clientReply map[int64]CommandContext		// 存客户端历史响应的哈希表。key为客户端id，value为"commandSeq + 响应"
	lastApplied int								// 上一条应用的log的index,防止快照导致回退

	// 客户端并行地发送请求时，不同的请求的command应用通知应该对应不同管道（对应这里的chan ApplyNotifyMsg），因此用一个哈希表进行区分。
	// key为该command提交时在raft log中的index, Value 为KVserver等待此index对应command的管道。
	// 流程应该是这样的：服务层接收到raft applyCh管道中传来的提交信息，然后应用其命令到状态机，再由"ApplyCh chan ApplyNotifyMsg"这一管道将应用完成的信息传给KVserver
	replyChMap map[int]chan ApplyNotifyMsg		// 某index的命令或是快照应用完成的通知管道

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args)

	// 判断该命令是否执行过了
	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		if commandContext.CommandSeq >= args.CommandSeq {
			reply.Err = commandContext.Reply.Err
			reply.Value = commandContext.Reply.Value
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	op := Op{										// 构造操作命令
		CommandType: GetMethod,
		ClientId: args.ClientId,
		CommandSeq: args.CommandSeq,
		Key: args.Key,
	}
	index, _, isleader := kv.rf.Start(op)		// 接收命令传给Raft层进行日志一致性复制
	DPrintf("kvserver[%d] isLeader:%v", kv.me, isleader)
	if !isleader {									// 判断自己是不是leader
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	replyCh := kv.getNotifyCh(index)					// 用于接收该命令被应用过的响应消息
	DPrintf("kvserver[%d]: 创建reply通道:index=[%d], args=[%v]\n", kv.me, index, args)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.replyChMap, index)
		kv.mu.Unlock()
	}()

	// 等待应用到状态机后返回信息
	select {
	case replyMsg := <-replyCh:
		DPrintf("kvserver[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", kv.me, index, replyMsg)
		reply.Err = replyMsg.Err
		reply.Value = replyMsg.Value
	case <- time.After(500*time.Millisecond):
		DPrintf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeout
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("kvserver[%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.me, args)

	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		if commandContext.CommandSeq >= args.CommandSeq {
			reply.Err = commandContext.Reply.Err
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	op := Op{										// 构建操作命令
		CommandType: CommandType(args.Op),
		ClientId: args.ClientId,
		CommandSeq: args.CommandSeq,
		Key: args.Key,
		Value: args.Value,
	}
	index, _, isleader := kv.rf.Start(op)		// 接收命令传给Raft层进行日志一致性复制
	DPrintf("kvserver[%d] isLeader:%v", kv.me, isleader)
	if !isleader {									// 判断自己是不是leader
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	replyCh := kv.getNotifyCh(index)					// 用于接收该命令被应用过的响应消息
	DPrintf("kvserver[%d]: 创建reply通道:index=[%d], args=[%v]\n", kv.me, index, args)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.replyChMap, index)
		kv.mu.Unlock()
	}()

	// 等待应用到状态机后返回信息
	select {
	case replyMsg := <-replyCh:
		DPrintf("kvserver[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", kv.me, index, replyMsg)
		reply.Err = replyMsg.Err

	case <- time.After(500*time.Millisecond):
		DPrintf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeout
	}
}

// 获取replyCh
func (kv *KVServer) getNotifyCh(index int) chan ApplyNotifyMsg {
	if _, ok := kv.replyChMap[index]; !ok {
		kv.replyChMap[index] = make(chan ApplyNotifyMsg)
	}
	return kv.replyChMap[index]
}


// 清除replyCh管道
func (kv *KVServer) closeChan(index int)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.replyChMap[index]
	if !ok{
		//若该index没有对应通道,直接结束
		DPrintf("kvserver[%d]: 无该通道index: %d\n", kv.me, index)
		return
	}
	close(ch)
	delete(kv.replyChMap, index)
	DPrintf("kvserver[%d]: 成功删除 index=%d 对应的通道\n", kv.me, index)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[]包含一组服务器的端口，这些服务器将通过与Raft交互形成容错key/value服务。me为servers[]中当前服务器的索引。
// k/v服务器应该通过底层Raft实现来存储快照，它应该调用persister.SaveStateAndSnapshot()来自动保存Raft状态和快照。
// 当Raft的保存状态超过maxraftstate字节时，k/v服务器应该快照，以便允许Raft对其日志进行垃圾收集。如果maxraftstate为-1，则不需要快照。
// StartKVServer()必须快速返回，因此它应该为任何长时间运行的工作启动例程。
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// 对你希望用Go的RPC库进行marshall/unmarshall（序列化/反序列化）的结构调用labgob.Register()
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDatabase = KVDatabase{make(map[string]string)}
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.clientReply = make(map[int64]CommandContext)
	kv.lastApplied = -1

	// 从快照中恢复数据
	kv.mu.Lock()
	kv.readSnapshot(kv.rf.GetSnapshot())
	kv.mu.Unlock()
	
	go kv.Notifier()						// 另起go程随时监听Raft的applyCh，并处理其中的消息

	DPrintf("start a new server[%d]", kv.me)
	return kv
}

// 随时监听管道中是否有command要应用或是否有snapshot传过来，对其处理
func (kv *KVServer) Notifier()  {
	for !kv.killed(){
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("KVserver[%d]:获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg)
			if applyMsg.CommandValid {					// command有效时
				kv.ApplyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {			// snapshot有效时
				kv.ApplySnapshot(applyMsg)
			} else {									// 不合法时
				DPrintf("KVserver[%d]: error applyMsg from applyCh: [%v]", kv.me, applyMsg)
			}
		}
	}
}


// 应用日志中的命令
func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if applyMsg.CommandIndex <= kv.lastApplied {
		DPrintf("kvserver[%d]:过期的applyMsg=[%v], 因为已恢复了lastApplied为%v的新快照", kv.me, applyMsg, kv.lastApplied)
		return
	}
	kv.lastApplied = applyMsg.CommandIndex

	var commonReply ApplyNotifyMsg
	op := applyMsg.Command.(Op)								// (Op) 表示将 applyMsg.Command 这个接口类型的变量断言为 Op 类型
	// 如果命令已经被应用过了
	if commandContext, ok := kv.clientReply[op.ClientId]; ok {
		if commandContext.CommandSeq >= op.CommandSeq {
			DPrintf("kvserver[%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", kv.me, applyMsg, commandContext)
			commonReply = commandContext.Reply
			return
		}
	}

	// 如果命令没被应用过
	if op.CommandType == GetMethod {						// Get 请求
		if value, ok := kv.kvDatabase.Get(op.Key); ok {
			// 如果有该Key对应的数据
			commonReply = ApplyNotifyMsg{
				Err: OK,
				Value: value,
			}
		} else {
			// 如果没有该Key对应的数据
			commonReply = ApplyNotifyMsg{
				Err: ErrNoKey,
				Value: value,
			}
		}
	} else if op.CommandType == PutMethod {					// Put请求
		value := kv.kvDatabase.Put(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{
			Err: OK,
			Value: value,
		}
	} else if op.CommandType == AppendMethod {				// Append请求
		newValue := kv.kvDatabase.Append(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{
			Err: OK,
			Value: newValue,
		}
	}

	// 将给客户端的回复保存
	kv.clientReply[op.ClientId] = CommandContext{
		CommandSeq: op.CommandSeq,
		Reply: commonReply,
	}
	DPrintf("kvserver[%d]: 更新客户端回复表, ClientId=%v, commandSeq=%v, reply=%v\n", kv.me, op.ClientId, op.CommandSeq, commonReply)

	// 只有leader能回复client，因此只有leader需要进行reply消息通知
	if term, isLeader := kv.rf.GetState(); isLeader && applyMsg.CommandTerm == term{
		replyCh := kv.getNotifyCh(applyMsg.CommandIndex)
		kv.mu.Unlock()
		replyCh <- commonReply					// 向通知管道发送消息
		kv.mu.Lock()
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,向index=[%d]的channel发送通知\n", kv.me, applyMsg, applyMsg.CommandIndex)
	}

	// 是否需要进行快照
	if kv.needSnapshot() {
		kv.startSnapShot(applyMsg.CommandIndex)
	}
}


// 应用applyCh传来的快照
func (kv *KVServer) ApplySnapshot(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kvserver[%d]: 收到raft层leader发来的快照", kv.me)

	if kv.lastApplied >= applyMsg.SnapshotIndex {
		DPrintf("kvserver[%d]: 这是过期快照, 状态机已应用:%d, snapshotIndex:%d", kv.me, kv.lastApplied, applyMsg.SnapshotIndex)
		return
	}

	if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
		kv.lastApplied = applyMsg.SnapshotIndex
		kv.readSnapshot(applyMsg.Snapshot)				// 读取快照应用到状态机
		DPrintf("kvserver[%d]: 完成服务层快照\n", kv.me)
	}
}


// 判断raft是否需要进行快照
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	proportion := float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion > 0.9
}

// 进行快照
func (kv *KVServer) startSnapShot(index int)  {
	DPrintf("kvserver[%d]: 容量超过阈值, 进行快照", kv.me)
	snapshot := kv.creatSnapshot()
	DPrintf("kvserver[%d]: 完成服务层快照", kv.me)
	go kv.rf.Snapshot(index, snapshot)			// 让raft层进行快照
}

// 构建快照
func (kv *KVServer) creatSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据
	err := e.Encode(kv.kvDatabase)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode kvData error: %v\n", kv.me, err)
	}
	//编码clientReply(为了去重)
	err = e.Encode(kv.clientReply)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode clientReply error: %v\n", kv.me, err)
	}
	snapshotData := w.Bytes()
	return snapshotData
}


// 读取快照恢复状态机
func (kv *KVServer) readSnapshot(snapshot []byte)  {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvdatabase KVDatabase
	var clientreply map[int64]CommandContext
	if d.Decode(&kvdatabase) != nil || d.Decode(&clientreply) != nil{
		log.Fatal("读取快照失败\n")
	} else {
	  kv.kvDatabase = kvdatabase
	  kv.clientReply = clientreply
	}
}
