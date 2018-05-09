package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
    ErrLeaderChanged = "ErrLeaderChanged"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    ClerkId int64
    CommandNum int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    ClerkId int64
    CommandNum int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type reconfigArgs struct {
    Config           shardmaster.Config
    ExpectShardsList []int
}

type MigrateShardsArgs struct {
    Num        int
    ShardsList []int
    KVPairs    map[string]string
    DupDtn     map[int64]int
    ClerkId    int64
    CommandNum int
}

type MigrateShardsReply struct {
    WrongLeader bool
    Err         Err
}

type KvState struct {
    KvStore        map[string]string
    RcvdCmd        map[int64]int
    CurrentConfig  shardmaster.Config
}
