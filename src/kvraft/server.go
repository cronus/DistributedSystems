package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Type string
    Key string
    Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    kvStore map[string]string
    msgBuffer []raft.ApplyMsg
    cond *sync.Cond
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    DPrintf("[kvserver: %v]Get args: %v\n", kv.me, args)

    op := Op{
        Type  : "Get",
        Key   : args.Key,
        Value : ""}

    index, term, isLeader := kv.rf.Start(op) 
    DPrintf("[kvserver: %v]Get, index: %v, term: %v, isLeader: %v\n", kv.me, index, term, isLeader)

    if !isLeader {
        reply.WrongLeader = true
        reply.Value       = ""
        return
    } else {
        reply.WrongLeader = false
    }

    kv.mu.Lock()
    defer kv.mu.Unlock()
    defer DPrintf("[kvserver: %v]Get index: %v, reply: %v\n", kv.me, index, reply)

    // wait majority peers agree
    for {
        if len(kv.msgBuffer) == 0 || index > kv.msgBuffer[0].CommandIndex {
            kv.cond.Wait()
        } else if index == kv.msgBuffer[0].CommandIndex {
            if op == kv.msgBuffer[0].Command {
                break
            } else {
                DPrintf("[kvserver: %v]Get Leader has changed\n", kv.me)
                reply.Err = ErrLeaderChanged
                reply.Value = ""
                return
            }
        }
    }

    DPrintf("[kvserver: %v]Get applyMsg: %v\n", kv.me, kv.msgBuffer[0])
    kv.msgBuffer = kv.msgBuffer[1:]
    kv.cond.Broadcast()

    if value, exist := kv.kvStore[args.Key]; exist {
        reply.Err   = OK
        reply.Value = value
    } else {
        reply.Err   = ErrNoKey
        reply.Value = ""
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    DPrintf("[kvserver: %v]PutAppend args: %v\n", kv.me, args)

    op := Op{
        Type  : args.Op,
        Key   : args.Key,
        Value : args.Value}

    index, term, isLeader := kv.rf.Start(op) 
    DPrintf("[kvserver: %v]PutAppend, index: %v, term: %v, isLeader: %v\n", kv.me, index, term, isLeader)
    
    if !isLeader {
        reply.WrongLeader = true
        return
    } else {
        reply.WrongLeader = false
    }

    kv.mu.Lock()
    defer kv.mu.Unlock()
    defer DPrintf("[kvserver: %v]PutAppend index: %v, reply: %v\n", kv.me, index, reply)

    // wait majority peers agree
    for {
        if len(kv.msgBuffer) == 0 || index > kv.msgBuffer[0].CommandIndex {
            kv.cond.Wait()
        } else if index == kv.msgBuffer[0].CommandIndex {
            if op == kv.msgBuffer[0].Command {
                reply.Err = OK
                break
            } else {
                DPrintf("[kvserver: %v]PutAppend Leader has changed\n", kv.me)
                reply.Err = ErrLeaderChanged
                return
            }
        }
    }

    DPrintf("[kvserver: %v]PutAppend applyMsg: %v\n", kv.me, kv.msgBuffer[0])
    kv.msgBuffer = kv.msgBuffer[1:]
    kv.cond.Broadcast()

    switch args.Op {
    case "Put":
        kv.kvStore[args.Key] = args.Value
    case "Append":
        kv.kvStore[args.Key] += args.Value
    }
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
    close(kv.applyCh)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
    kv.kvStore = make(map[string]string)
    kv.msgBuffer = make([]raft.ApplyMsg, 0)
    kv.cond = sync.NewCond(&kv.mu)

    go func(kv *KVServer) {
        for msg := range kv.applyCh {
            kv.mu.Lock()
            //if len(kv.msgBuffer) != 0 {
            //    kv.cond.Wait()
            //}
            kv.msgBuffer = append(kv.msgBuffer, msg)
            DPrintf("[kvserver: %v]Receive applyMsg from raft: %v\n", kv.me, msg)
            kv.cond.Broadcast()
            kv.mu.Unlock()
        }
    }(kv)

	return kv
}
