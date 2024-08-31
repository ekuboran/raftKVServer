package shardctrler


import (
	"time"
	"6.824/raft"
	"6.824/labrpc"
	"sync"
	"6.824/labgob"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	clientReply map[int64]OperationContext
	replyChMap map[int]chan CommandReply
}

type OperationContext struct {			// 用于存储对客户端的响应
	CommandSeq int			// 该client目前的commandSeq
	Reply CommandReply		// 对应该commandSeq的响应
}

type Op struct {
	// Your data here.
	Operation string			// 操作类型
	ClientId int64				// client唯一标识
	CommandSeq int				// 该command的序号
	Servers map[int][]string	// for Join	. new GID -> servers mappings
	GIDs []int					// for Leave
	Shard int					// for Move
	GID   int					// for Move
	Num int						// for Query . desired config number
}

func (sc *ShardCtrler) Join(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("shardctrler[%d]: 接收%v RPC请求,args=[%v]\n", sc.me, args.Op,args)
	// 判断该操作是否执行过了
	if operationContext, ok := sc.clientReply[args.ClientId]; ok {
		if operationContext.CommandSeq >= args.CommandSeq {
			reply.Err = operationContext.Reply.Err
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()
	op := Op{								// 构造操作命令
		Operation: JoinMethod,
		CommandSeq: args.CommandSeq,
		ClientId: args.ClientId,
		Servers: args.Servers,
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	replyCh := sc.getNotifyCh(index)					// 用于接收该命令被应用过的响应消息
	DPrintf("shardctrler[%d]: 创建reply通道:index=[%d], args=[%v]\n", sc.me, index, args)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.replyChMap, index)
		sc.mu.Unlock()
	}()

	select {
	case replyMsg := <-replyCh:
		DPrintf("shardctrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
		reply.Err = replyMsg.Err
	case <- time.After(500*time.Millisecond):
		DPrintf("shardctrler[%d]: 处理请求超时: %v\n", sc.me, op)
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Leave(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("shardctrler[%d]: 接收%v RPC请求,args=[%v]\n", sc.me, args.Op,args)
	// 判断该操作是否执行过了
	if operationContext, ok := sc.clientReply[args.ClientId]; ok {
		if operationContext.CommandSeq >= args.CommandSeq {
			reply.Err = operationContext.Reply.Err
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()
	op := Op{								// 构造操作命令
		Operation: LeaveMethod,
		CommandSeq: args.CommandSeq,
		ClientId: args.ClientId,
		GIDs: args.GIDs,
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	replyCh := sc.getNotifyCh(index)					// 用于接收该命令被应用过的响应消息
	DPrintf("shardctrler[%d]: 创建reply通道:index=[%d], args=[%v]\n", sc.me, index, args)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.replyChMap, index)
		sc.mu.Unlock()
	}()

	select {
	case replyMsg := <-replyCh:
		DPrintf("shardctrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
		reply.Err = replyMsg.Err
	case <- time.After(500*time.Millisecond):
		DPrintf("shardctrler[%d]: 处理请求超时: %v\n", sc.me, op)
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Move(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("shardctrler[%d]: 接收%v RPC请求,args=[%v]\n", sc.me, args.Op,args)
	// 判断该操作是否执行过了
	if operationContext, ok := sc.clientReply[args.ClientId]; ok {
		if operationContext.CommandSeq >= args.CommandSeq {
			reply.Err = operationContext.Reply.Err
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()
	op := Op{								// 构造操作命令
		Operation: MoveMethod,
		CommandSeq: args.CommandSeq,
		ClientId: args.ClientId,
		Shard: args.Shard,
		GID: args.GID,
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	replyCh := sc.getNotifyCh(index)					// 用于接收该命令被应用过的响应消息
	DPrintf("shardctrler[%d]: 创建reply通道:index=[%d], args=[%v]\n", sc.me, index, args)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.replyChMap, index)
		sc.mu.Unlock()
	}()

	select {
	case replyMsg := <-replyCh:
		DPrintf("shardctrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
		reply.Err = replyMsg.Err
	case <- time.After(500*time.Millisecond):
		DPrintf("shardctrler[%d]: 处理请求超时: %v\n", sc.me, op)
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Query(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("shardctrler[%d]: 接收%v RPC请求,args=[%v]\n", sc.me, args.Op,args)
	// 判断该操作是否执行过了
	if operationContext, ok := sc.clientReply[args.ClientId]; ok {
		if operationContext.CommandSeq >= args.CommandSeq {
			reply.Err = operationContext.Reply.Err
			reply.Config = operationContext.Reply.Config
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()
	op := Op{								// 构造操作命令
		Operation: QueryMethod,
		CommandSeq: args.CommandSeq,
		ClientId: args.ClientId,
		Num: args.Num,
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	replyCh := sc.getNotifyCh(index)					// 用于接收该命令被应用过的响应消息
	DPrintf("shardctrler[%d]: 创建reply通道:index=[%d], args=[%v]\n", sc.me, index, args)
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.replyChMap, index)
		sc.mu.Unlock()
	}()

	select {
	case replyMsg := <-replyCh:
		DPrintf("shardctrler[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", sc.me, index, replyMsg)
		reply.Err = replyMsg.Err
		reply.Config = replyMsg.Config
	case <- time.After(500*time.Millisecond):
		DPrintf("shardctrler[%d]: 处理请求超时: %v\n", sc.me, op)
		reply.Err = ErrTimeout
	}
}


// 获取replyCh
func (sc *ShardCtrler) getNotifyCh(index int) chan CommandReply {
	if _, ok := sc.replyChMap[index]; !ok {
		sc.replyChMap[index] = make(chan CommandReply)
	}
	return sc.replyChMap[index]
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientReply = make(map[int64]OperationContext)
	sc.replyChMap = make(map[int]chan CommandReply)

	go sc.Notifier()
	return sc
}


func (sc *ShardCtrler) Notifier()  {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {					// command有效时
				sc.ApplyCommand(applyMsg)
			}
		}
	}
}

func (sc *ShardCtrler) ApplyCommand(applyMsg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	var commonReply CommandReply
	op := applyMsg.Command.(Op)

	// 如果命令已经被应用过了
	if operationContext, ok := sc.clientReply[op.ClientId]; ok {
		if operationContext.CommandSeq >= op.CommandSeq {
			DPrintf("shardctrler[%d]: 该命令已被应用过,applyMsg: %v, operationContext: %v\n", sc.me, applyMsg, operationContext)
			commonReply = operationContext.Reply
			return
		}
	}
	// 如果命令没应用过
	if op.Operation == JoinMethod {
		sc.executeJoin(op.Servers)
		commonReply = CommandReply{
			Err: OK,
		}
	} else if op.Operation == LeaveMethod {
		sc.executeLeave(op.GIDs)
		commonReply = CommandReply{
			Err: OK,
		}
	} else if op.Operation == MoveMethod {
		sc.executeMove(op.Shard, op.GID)
		commonReply = CommandReply{
			Err: OK,
		}
	} else if op.Operation == QueryMethod {
		config := sc.executeQuery(op.Num)
		commonReply = CommandReply{
			Err: OK,
			Config: config,
		}
	}
	DPrintf("shardctrler[%d]:最新的configs.shards:%v", sc.me,sc.configs[len(sc.configs)-1].Shards)
	// 将给客户端的回复保存
	sc.clientReply[op.ClientId] = OperationContext{
		CommandSeq: op.CommandSeq,
		Reply: commonReply,
	}
	DPrintf("shardctrler[%d]: 更新客户端回复表, ClientId=%v, commandSeq=%v, reply=%v\n", sc.me, op.ClientId, op.CommandSeq, commonReply)

	if term, isLeader := sc.rf.GetState(); isLeader && applyMsg.CommandTerm == term{
		
		replyCh := sc.getNotifyCh(applyMsg.CommandIndex)
		sc.mu.Unlock()
		DPrintf("shardctrler[%d]: applyMsg: %v处理完成,向index=[%d]的channel发送通知\n", sc.me, applyMsg, applyMsg.CommandIndex)
		replyCh <- commonReply					// 向通知管道发送消息
		sc.mu.Lock()
	}
}