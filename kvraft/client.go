package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import (
	mathrand "math/rand"
	// "time"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64		// 客户端的唯一标识
	commandSeq int		// 当前最大请求的序列号
	lastLeader int		// 上一次RPC请求所知的leader id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()					// 每个客户端的唯一标识由nrad()生成
	ck.commandSeq = 0
	ck.lastLeader = mathrand.Intn(len(ck.servers))
	return ck
}

//
// fetch the current value for a key.
// 如果不存在key，返回""
// 如果是其他的错误，永远坚持尝试。
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// args和reply的类型(包括它们是否为指针)必须与RPC处理程序函数的参数声明的类型匹配。而reply必须作为指针传递。
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		CommandSeq: ck.commandSeq + 1,
	}

	serverId := ck.lastLeader
	serverNum := len(ck.servers)
	defer func ()  {
		ck.commandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()

	for {
		var reply GetReply
		DPrintf("client[%d]: 向server[%d]发送Get RPC. args=[%v]\n", ck.clientId, serverId, args)
		ok := ck.sendGet(serverId, &args, &reply)
		if ok {
			if reply.Err == OK {
				//接收到正确的reply
				DPrintf("client[%d]: 发送给server[%d]的Get RPC成功.args=[%v], ok=[%v], Reply=[%v]\n", ck.clientId, serverId, args, ok, reply)
				ck.lastLeader = serverId
				return reply.Value
			} else if reply.Err == ErrNoKey {			// 没有该key，返回""
				return ""
			} else if reply.Err == ErrTimeout || reply.Err == ErrWrongLeader{
				serverId = (serverId + 1) % serverNum
				continue
			}
		}
		serverId = (serverId + 1) % serverNum
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok	
}

//
// 由Put和Append共享
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// args和reply的类型(包括它们是否为指针)必须与RPC处理程序函数的参数声明的类型匹配。而reply必须作为指针传递。
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		CommandSeq: ck.commandSeq + 1,
	}
	serverId := ck.lastLeader
	serverNum := len(ck.servers)
	defer func ()  {
		ck.commandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()

	for {
		var reply PutAppendReply
		DPrintf("client[%d]: 向server[%d]发送PutAppend RPC. args=[%v]\n", ck.clientId, serverId, args)
		ok := ck.sendPutAppend(serverId, &args, &reply)
		if ok {
			if reply.Err == OK {
				//接收到正确的reply
				DPrintf("client[%d]: 发送给server[%d]的PutAppend RPC成功.args=[%v], ok=[%v], Reply=[%v]\n", ck.clientId, serverId, args, ok, reply)
				ck.lastLeader = serverId
				return
			} else if reply.Err == ErrTimeout || reply.Err == ErrWrongLeader{
				serverId = (serverId + 1) % serverNum
				continue
			}
		}
		serverId = (serverId + 1) % serverNum
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
