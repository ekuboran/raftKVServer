package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

// 查找key在哪个shard中
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64		// 客户端的唯一标识
	commandSeq int		// 当前最大请求的序列号
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.config = ck.sm.Query(-1)
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.commandSeq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		CommandSeq: ck.commandSeq + 1,
	}
	defer func ()  {
		ck.commandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					if reply.Err == OK{
						return reply.Value
					} else if reply.Err == ErrNoKey {
						return ""
					} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
						continue
					} else if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		CommandSeq: ck.commandSeq + 1,
	}
	defer func ()  {
		ck.commandSeq ++						// 发给leader并正确返回后对commandSeq修改
	}()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					if reply.Err == OK{
						return
					} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
						continue
					} else if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
