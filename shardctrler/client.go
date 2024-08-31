package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId int64
	CommandSeq int
	leaderId int 
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
	// Your code here.
	ck.ClientId = nrand()
	ck.CommandSeq = 0
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

func (ck *Clerk) Query(num int) Config {
	
	// Your code here.
	defer func ()  {
		ck.CommandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()
	args := CommandArgs{
		Num: num,
		Op: QueryMethod,
		ClientId: ck.ClientId,
		CommandSeq: ck.CommandSeq+1,
	}
	serverId := ck.leaderId
	for {
		var reply CommandReply
		DPrintf("client[%d]: 向server[%d]Query RPC. args=[%v]\n", ck.ClientId, serverId, args)
		ok := ck.servers[serverId].Call("ShardCtrler.Query", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Config
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	defer func ()  {
		ck.CommandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()
	args := CommandArgs{
		Servers: servers,
		Op: JoinMethod,
		ClientId: ck.ClientId,
		CommandSeq: ck.CommandSeq+1,
	}
	serverId := ck.leaderId
	for {
		var reply CommandReply
		DPrintf("client[%d]: 向server[%d]Join RPC. args=[%v]\n", ck.ClientId, serverId, args)
		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	defer func ()  {
		ck.CommandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()
	args := CommandArgs{
		GIDs: gids,
		Op: LeaveMethod,
		ClientId: ck.ClientId,
		CommandSeq: ck.CommandSeq+1,
	}
	serverId := ck.leaderId
	for {
		var reply CommandReply
		DPrintf("client[%d]: 向server[%d]Leave RPC. args=[%v]\n", ck.ClientId, serverId, args)
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	defer func ()  {
		ck.CommandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()
	args := CommandArgs{
		Shard: shard,
		GID: gid,
		Op: MoveMethod,
		ClientId: ck.ClientId,
		CommandSeq: ck.CommandSeq+1,
	}
	serverId := ck.leaderId
	for {
		var reply CommandReply
		DPrintf("client[%d]: 向server[%d]Move RPC. args=[%v]\n", ck.ClientId, serverId, args)
		ok := ck.servers[serverId].Call("ShardCtrler.Move", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
