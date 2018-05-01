package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "time"
    "fmt"
    "bytes"
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
    ClerkId int64
    CommandNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    isLeader bool
    bufferTerm int

    // persist when snapshot
    kvStore map[string]string
    receivedCmd map[int64]int

    // need to initial when becoming Leader
    initialIndex int
    msgBuffer []raft.ApplyMsg

    cond *sync.Cond
    shutdown chan struct{}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    DPrintf("[kvserver: %v]Get args: %v\n", kv.me, args)

    // check if the command is committed
    //kv.mu.Lock()
    //_, isLeader := kv.rf.GetState()
    //if isLeader {
    //    if num, ok := kv.receivedCmd[args.ClerkId]; ok && num == args.CommandNum {
    //        DPrintf("[kvserver: %v]Get, command %v is already committed.\n", kv.me, args)
    //        reply.WrongLeader = false
    //        reply.Err = OK
    //        reply.Value = kv.kvStore[args.Key]
    //        kv.mu.Unlock()
    //        return
    //    } else {
    //        kv.receivedCmd[args.ClerkId] = -1 
    //        DPrintf("[kvserver: %v]Get, command %v firstly recevied.\n", kv.me, args)
    //    }
    //}
    //kv.mu.Unlock()

    // lock at the beginning to make sure the smallest index reset the msgBuffer
    kv.mu.Lock()
    // wait a while for agreement
    defer time.Sleep(50 * time.Millisecond)
    defer kv.mu.Unlock()
    defer DPrintf("[kvserver: %v]Get args: %v, reply: %v\n", kv.me, args, reply)

    op := Op{
        Type       : "Get",
        Key        : args.Key,
        Value      : "",
        ClerkId    : args.ClerkId,
        CommandNum : args.CommandNum}

    index, term1, isLeader := kv.rf.Start(op) 
    DPrintf("[kvserver: %v]Get, index: %v, term: %v, isLeader: %v\n", kv.me, index, term1, isLeader)

    if !isLeader {
        reply.WrongLeader = true
        reply.Err         = ErrWrongLeader
        reply.Value       = ""
        return
    } else {
        reply.WrongLeader = false
    }


    // clear fifo if a new leader
    if !kv.isLeader && isLeader || kv.bufferTerm != term1 {
        kv.initialIndex = index
        kv.msgBuffer    = kv.msgBuffer[:0]
        DPrintf("[kvserver: %v] Get, new Leader, clear buffer\n", kv.me)
        kv.isLeader     = isLeader
        kv.bufferTerm   = term1
    }

    // wait majority peers agree
    for {
        if len(kv.msgBuffer) == 0 || index > kv.msgBuffer[0].CommandIndex {
            if len(kv.msgBuffer) != 0 {
                DPrintf("[kvserver: %v]Get, index not match: allocated %v > index in buffer %v\n", kv.me, index, kv.msgBuffer[0].CommandIndex)
            } else {
                DPrintf("[kvserver: %v]Get, msgBuffer is empty\n", kv.me)
            }
            term2, isLeader := kv.rf.GetState()
            DPrintf("[kvserver: %v]Get term2: %v, isLeader: %v\n", kv.me, term2, isLeader)
            if !isLeader || term1 != term2 {
                DPrintf("[kvserver: %v]Get Leader has changed when NOT read the msg\n", kv.me)
                reply.Err   = ErrLeaderChanged
                reply.Value = ""
                kv.isLeader = isLeader
                return
            }
            kv.cond.Wait()
        } else if index == kv.msgBuffer[0].CommandIndex {
            DPrintf("[kvserver: %v]Get, index match: %v\n", kv.me, index)
            term3, isLeader := kv.rf.GetState()
            if isLeader && term1 == term3 && op == kv.msgBuffer[0].Command {
                break
            } else {
                DPrintf("[kvserver: %v]Get Leader has changed when read the msg\n", kv.me)
                reply.Err    = ErrLeaderChanged
                reply.Value  = ""
                kv.isLeader  = isLeader
                kv.msgBuffer = kv.msgBuffer[1:]
                kv.cond.Broadcast()
                return
            }
        } else {
            DPrintf("[kvserver: %v]Get, index %v < index in buffer %v\n", kv.me, index, kv.msgBuffer[0].CommandIndex)
            term4, isLeader := kv.rf.GetState()
            DPrintf("[kvserver: %v]Get term4: %v, isLeader: %v\n", kv.me, term4, isLeader)
            if !isLeader || term1 != term4 {
                DPrintf("[kvserver: %v]Get Leader has changed when post read the msg\n", kv.me)
                reply.Err   = ErrLeaderChanged
                reply.Value = ""
                kv.isLeader = isLeader
                return
            } else {
                err := fmt.Sprintf("[kvserver: %v]Get index: %v smaller than msgBuffer[0].CommandIndex: %v, but still leader and same term", kv.me, index, kv.msgBuffer[0].CommandIndex)
                panic(err)
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

    // check if the command is committed
    //kv.mu.Lock()
    //_, isLeader := kv.rf.GetState()
    //if isLeader {
    //    if num, ok := kv.receivedCmd[args.ClerkId]; ok && num == args.CommandNum {
    //        DPrintf("[kvserver: %v]PutAppend, command %v is already committed.\n", kv.me, args)
    //        reply.WrongLeader = false
    //        reply.Err = OK
    //        kv.mu.Unlock()
    //        return
    //    } else {
    //        kv.receivedCmd[args.ClerkId] = -1
    //        DPrintf("[kvserver: %v]PutAppend, command %v firstly recevied.\n", kv.me, args)
    //    }
    //}
    //kv.mu.Unlock()

    // lock at the beginning to make sure the smallest index reset the msgBuffer
    kv.mu.Lock()
    // wait a while for agreement
    defer time.Sleep(50 * time.Millisecond)
    defer kv.mu.Unlock()
    defer DPrintf("[kvserver: %v]PutAppend args, %v, reply: %v\n", kv.me, args, reply)

    op := Op{
        Type       : args.Op,
        Key        : args.Key,
        Value      : args.Value,
        ClerkId    : args.ClerkId,
        CommandNum : args.CommandNum}

    index, term1, isLeader := kv.rf.Start(op) 
    DPrintf("[kvserver: %v]PutAppend, index: %v, term: %v, isLeader: %v\n", kv.me, index, term1, isLeader)
    
    if !isLeader {
        reply.WrongLeader = true
        reply.Err = ErrWrongLeader
        return
    } else {
        reply.WrongLeader = false
    }


    // clear fifo if a new leader
    if !kv.isLeader && isLeader || kv.bufferTerm != term1 {
        kv.initialIndex = index
        kv.msgBuffer    = kv.msgBuffer[:0]
        DPrintf("[kvserver: %v] PutAppend, new Leader, clear buffer\n", kv.me)
        kv.isLeader     = isLeader
        kv.bufferTerm   = term1
    }

    // wait majority peers agree
    for {
        if len(kv.msgBuffer) == 0 || index > kv.msgBuffer[0].CommandIndex {
            if len(kv.msgBuffer) != 0 {
                DPrintf("[kvserver: %v]PutAppend, index not match: allocated %v > index in buffer %v\n", kv.me, index, kv.msgBuffer[0].CommandIndex)
            } else {
                DPrintf("[kvserver: %v]PutAppend, msgBuffer is empty\n", kv.me)
            }
            term2, isLeader := kv.rf.GetState()
            DPrintf("[kvserver: %v]PutAppend term2: %v, isLeader: %v\n", kv.me, term2, isLeader)
            if !isLeader || term1 != term2 {
                DPrintf("[kvserver: %v]PutAppend Leader has changed when NOT read the msg\n", kv.me)
                reply.Err   = ErrLeaderChanged
                kv.isLeader = isLeader
                return
            }
            kv.cond.Wait()
        } else if index == kv.msgBuffer[0].CommandIndex {
            DPrintf("[kvserver: %v]PutAppend, index match: %v\n", kv.me, index)
            term3, isLeader := kv.rf.GetState()
            if isLeader && term1 == term3 && op == kv.msgBuffer[0].Command {
                reply.Err = OK
                break
            } else {
                DPrintf("[kvserver: %v]PutAppend Leader has changed when read the msg\n", kv.me)
                reply.Err    = ErrLeaderChanged
                kv.isLeader  = isLeader
                kv.msgBuffer = kv.msgBuffer[1:]
                kv.cond.Broadcast()
                return
            }
        } else {
            DPrintf("[kvserver: %v]PutAppend, index %v < index in buffer %v\n", kv.me, index, kv.msgBuffer[0].CommandIndex)
            term4, isLeader := kv.rf.GetState()
            DPrintf("[kvserver: %v]PutAppend term4: %v, isLeader: %v\n", kv.me, term4, isLeader)
            if !isLeader || term1 != term4 {
                DPrintf("[kvserver: %v]PutAppend Leader has changed when post read the msg\n", kv.me)
                reply.Err   = ErrLeaderChanged
                kv.isLeader = isLeader
                return
            } else {
                err := fmt.Sprintf("[kvserver: %v]PutAppend index: %v smaller than msgBuffer[0].CommandIndex: %v, but still leader and same term", kv.me, index, kv.msgBuffer[0].CommandIndex)
                panic(err)
            }
        }
    }

    DPrintf("[kvserver: %v]PutAppend applyMsg: %v\n", kv.me, kv.msgBuffer[0])
    kv.msgBuffer = kv.msgBuffer[1:]
    kv.cond.Broadcast()
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

    // Wait for a while for servers to shutdown, since
    // shutdown isn't a real crash and isn't instantaneous
    time.Sleep(500 * time.Millisecond)
    kv.mu.Lock()
    defer kv.mu.Unlock()
    close(kv.applyCh)
    close(kv.shutdown)
    DPrintf("[kvserver: %v]Kill kv server\n", kv.me)
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
    kv.isLeader    = false
    kv.bufferTerm  = 0

    // store in snapshot
    kv.receivedCmd = make(map[int64]int)
    kv.kvStore     = make(map[string]string)
    kv.msgBuffer   = make([]raft.ApplyMsg, 0)

    kv.cond        = sync.NewCond(&kv.mu)
    kv.shutdown    = make(chan struct{})

    go func(kv *KVServer, persister *raft.Persister) {
        for msg := range kv.applyCh {
            if msg.CommandValid { 
                kv.mu.Lock()
                if kv.initialIndex <= msg.CommandIndex {
                    kv.msgBuffer = append(kv.msgBuffer, msg)
                }
                op := msg.Command.(Op)

                // duplicated command detection
                if num, ok := kv.receivedCmd[op.ClerkId]; ok && num == op.CommandNum {
                    DPrintf("[kvserver: %v]PutAppend, command %v is already committed.\n", kv.me, op)
                    kv.cond.Broadcast()
                    kv.mu.Unlock()
                    continue
                } 
                kv.receivedCmd[op.ClerkId] = op.CommandNum
                DPrintf("[kvserver: %v]Receive applyMsg from raft: %v\n", kv.me, msg)

                switch op.Type {
                case "Put":
                    kv.kvStore[op.Key] = op.Value
                case "Append":
                    kv.kvStore[op.Key] += op.Value
                }
                DPrintf("[kvserver: %v]kvStore: %v", kv.me, kv.kvStore)
                kv.cond.Broadcast()

                // detect when the persisted Raft state grows too large
                // hand a snapshot and tells Raft that it can discard old log entires
                // Raft should save with persist.SaveStateAndSnapshot()
                // kv server should restore the snapshot from the persister when it restarts
                if kv.maxraftstate != -1 && persister.RaftStateSize() > kv.maxraftstate {
                    // snapshot
                    buffer       := new(bytes.Buffer)
                    e            := labgob.NewEncoder(buffer)
                    e.Encode(kv.kvStore)
                    e.Encode(kv.receivedCmd)
                    snapshotData := buffer.Bytes()

                    // send snapshot to raft 
                    // tell it to discard logs and persist snapshot and remaining log
                    kv.rf.CompactLog(snapshotData, msg.CommandIndex)
                }
                kv.mu.Unlock()
            } else {
                kv.mu.Lock()
                // InstallSnapshot RPC
                DPrintf("[kvserver: %v]build state from InstallSnapShot: %v\n", kv.me, msg.Snapshot)
                kv.buildState(msg.Snapshot)
                kv.mu.Unlock()
            }

        }
    }(kv, persister)

    // periodically Broadcast to check term and leader
    go func(kv *KVServer) {
        for {
            select {
            case <- kv.shutdown:
                return
            default:
                time.Sleep(500 * time.Millisecond)
                kv.cond.Broadcast()
            }
        }
    }(kv)

//    go func(kv *KVServer, persister *raft.Persister, snapshotMsgCh chan raft.ApplyMsg) {
//        for {
//            kv.mu.Lock()
//            select {
//            case <- kv.shutdown:
//                kv.mu.Unlock()
//                return
//            case snapshotMsg :=<- snapshotMsgCh:
//                // snapshot
//                //if persister.RaftStateSize() > kv.maxraftstate {
//                buffer := new(bytes.Buffer)
//                e      := labgob.NewEncoder(buffer)
//                e.Encode(snapshotMsg.lastIndex)
//                e.Encode(snapshotMsg.lastTerm)
//                e.Encode(kv.kvStore)
//                snapshot := buffer.Bytes()
//
//                // send snapshot to raft 
//                // tell it to discard logs and persist snapshot and remaining log
//                kv.rf.CompactLog(snapshot, kv.lastIndex)
//                //}
//                kv.mu.Unlock()
//            }
//        }
//    }(kv, persister, snapshotMsgCh)

    // recover from snapshot after a reboot
    kv.buildState(persister.ReadSnapshot())

	return kv
}

func (kv *KVServer) buildState(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    //kv.mu.Lock()
    //defer kv.mu.Unlock()

    buffer := bytes.NewBuffer(data)
    d  := labgob.NewDecoder(buffer)

    //var lastIndex int
    //var lastTerm int
    //if err := d.Decode(&lastIndex); err != nil {
    //    panic(err)
    //}
    //if err := d.Decode(&lastTerm); err != nil {
    //    panic(err)
    //}

    if err := d.Decode(&kv.kvStore); err != nil {
        panic(err)
    }

    // restore receivedCmd
    if err := d.Decode(&kv.receivedCmd); err != nil {
        panic(err)
    }


    DPrintf("[kvserver: %v]After build kvServer state: kvstore: %v\n", kv.me, kv.kvStore)
}
