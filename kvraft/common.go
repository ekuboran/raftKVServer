package kvraft

import (
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout	   = "ErrTimeout"
	PutMethod      = "Put"
	AppendMethod   = "Append"
	GetMethod      = "Get"
)

type Err string

type CommandType string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	CommandSeq int
	// You'll have to add definitions here.
	// 字段名必须以大写字母开头，否则RPC将中断。
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	CommandSeq int
}

type GetReply struct {
	Err   Err
	Value string
}



