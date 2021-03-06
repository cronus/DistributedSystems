package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "log"
import "bytes"
import "time"

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
    Name string
    Args       interface{}
    ClerkId    int64
    CommandNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

    // for migrate shards
    mck              *shardmaster.Clerk
    currentConfig    shardmaster.Config
    expectShardsList []int
    inTransition     bool

    // persist when snapshot
    kvStore map[string]string
    rcvdCmd map[int64]int
    //rcvdKVCmd map[int64]int
    commandNum int

    // kvStoreBackup for garbage collection
    //kvStoreBackup map[string]string

    // need to initial when is the Leader
    initialIndex    int
    rtnChBuffer     []chan rtnMsg

    // for shutdown
    shutdown chan struct{}
}

type rfState struct {
    index    int
    term     int
    isLeader bool
    Err      Err
}

type rtnMsg struct {
    rfState rfState
    Err     Err
    Value   string
}

// function to check a key is ready during transition
//     more fine check,  for challenge2
//     1. to allow irrelevant key to proceceed
//     2. to allow received key to proceed
//func (kv *ShardKV) isReady(key string) bool {
//    var ready bool
//    if !kv.inTransition {
//        ready = true
//    } else {
//        ready = false
//    }
//
//    DPrintf("[kvserver: %v @ %v]check key: %v is ready: %v\n", kv.me, kv.gid, key, ready)
//    return ready
//}

// function to check key in group
func (kv *ShardKV) isInGroup(key string) bool {
    var inGroup bool
    if kv.inTransition {
        for _, shard := range kv.expectShardsList {
            if key2shard(key) == shard {
                return false
            }
        }
        keyShardIndex := key2shard(key)
        inGroup = (kv.gid == kv.currentConfig.Shards[keyShardIndex])
    } else {
        keyShardIndex := key2shard(key)
        inGroup = (kv.gid == kv.currentConfig.Shards[keyShardIndex])
    }

    return inGroup
}

// function to feed command to raft
func (kv *ShardKV) feedCmd(name string, args interface{}) rfState {

    var clerkId    int64
    var commandNum int
    var state rfState

    switch name {
    case "Get":
        a         := args.(GetArgs)
        clerkId    = a.ClerkId
        commandNum = a.CommandNum
    case "PutAppend":
        a         := args.(PutAppendArgs)
        clerkId    = a.ClerkId
        commandNum = a.CommandNum
    case "reconfig":
        // no action for pseudo command config
        clerkId    = -1
        commandNum = -1
    case "MigrateShards":
        a         := args.(MigrateShardsArgs)
        clerkId    = a.ClerkId
        commandNum = a.CommandNum
        
    default:
        panic("unexpected command!")
    }

    op := Op{
        Name       : name,
        Args       : args,
        ClerkId    : clerkId,
        CommandNum : commandNum}

    index, term, isLeader := kv.rf.Start(op)

    if len(kv.rtnChBuffer) == 0 {
        kv.initialIndex = index
    }

    state = rfState {
        index    : index,
        term     : term,
        isLeader : isLeader}

    defer DPrintf("[kvserver: %v @ %v]%v, args: %v state: %v\n", kv.me, kv.gid, name, args, state)
    return state

}

func (kv *ShardKV) checkRfState(oldState rfState, newState rfState) bool {

    var sameState bool

    if oldState.index != newState.index {
        DPrintf("[kvserver: %v @ %v]oldState: %v, newState: %v", kv.me, kv.gid, oldState, newState)
        panic("index not equal!")
    } else if oldState.isLeader != newState.isLeader {
        DPrintf("[kvserver: %v @ %v]not the leader\n", kv.me, kv.gid)
        sameState = false
    } else if oldState.term != newState.term {
        DPrintf("[kvserver: %v @ %v]term differ\n", kv.me, kv.gid)
        sameState = false
    } else {
        sameState = true
    }

    return sameState
}

func (kv *ShardKV) applyCmd(command Op) (string, Err) {

    DPrintf("[kvserver: %v @ %v]Apply command: %v", kv.me, kv.gid, command)

    var value string
    var Err Err
    value = ""
    Err = OK

    // duplicated command detection
    if num, ok := kv.rcvdCmd[command.ClerkId]; ok && num == command.CommandNum {
        DPrintf("[kvserver: %v @ %v]%v, command %v is already committed.\n", kv.me, kv.gid, command.Name, command)
        
        switch command.Name {
        case "PutAppend":
            args := command.Args.(PutAppendArgs)
            
            if !kv.isInGroup(args.Key) {
                Err = ErrWrongGroup
                return value, Err
            }

        case "Get":
            args := command.Args.(GetArgs)

            if !kv.isInGroup(args.Key) {
                Err = ErrWrongGroup
                return value, Err
            }

            // get value for Get command
            if v, exist := kv.kvStore[args.Key]; exist {
                Err   = OK
                value = v
            } else {
                Err   = ErrNoKey
                value = ""
            }
        }
    } else {
        switch command.Name {
        case "PutAppend":
            args := command.Args.(PutAppendArgs)

            if !kv.isInGroup(args.Key) {
                Err = ErrWrongGroup
                return value, Err
            }

            switch args.Op {
            case "Put":
                kv.kvStore[args.Key]  = args.Value
            case "Append":
                kv.kvStore[args.Key] += args.Value
            }
            kv.rcvdCmd[command.ClerkId] = command.CommandNum

        case "Get":
            args := command.Args.(GetArgs)
            if !kv.isInGroup(args.Key) {
                Err = ErrWrongGroup
                return value, Err
            }

            // get value for Get command
            if v, exist := kv.kvStore[args.Key]; exist {
                Err   = OK
                value = v
            } else {
                Err   = ErrNoKey
                value = ""
            }

            kv.rcvdCmd[command.ClerkId] = command.CommandNum
        }
    }

    // non-client commands
    switch command.Name {
    case "reconfig":

        
        args := command.Args.(reconfigArgs)
        kv.currentConfig = args.Config
        DPrintf("[kvserver: %v @ %v]update config: %v", kv.me, kv.gid, kv.currentConfig)

        // enter transition
        if len(args.ExpectShardsList) != 0 {
            kv.inTransition  = true
            // since slice is only reference, need to create a new slice
            //kv.expectShardsList = args.ExpectShardsList 
            kv.expectShardsList = make([]int, 0)
            for _, shard := range args.ExpectShardsList {
                kv.expectShardsList = append(kv.expectShardsList, shard)
            }
        }

        _, isLeader := kv.rf.GetState()
        if isLeader && !args.Sent[0]{
            // send to other groups
            for gid, shards := range args.SendMap {

                kvPairs := make(map[string]string)
                dupDtn  := make(map[int64]int)
                // find all the keys related to the gid and args.sendMap[gid]
                for _, shard := range shards {
                    for key, value := range kv.kvStore {
                        if shard == key2shard(key) {
                            kvPairs[key] = value
                        }
                    }
                }
                for k, v := range kv.rcvdCmd {
                    dupDtn[k] = v
                }

                args           := &MigrateShardsArgs{}
                args.Num        = kv.currentConfig.Num
                args.ShardsList = shards
                args.KVPairs    = kvPairs
                args.DupDtn     = dupDtn
                args.ClerkId    = int64(kv.gid)
                args.CommandNum = kv.commandNum

                kv.commandNum++

                DPrintf("[kvserver: %v @ %v]args send to other groups: %v\n", kv.me, kv.gid, args)

                //reply := new(MigrateShardsReply)

                go kv.sendMigrateShards(gid, args)
            }

        }

        //// copy kv.kvstore to kv.kvStoreBackup
        //kv.kvStoreBackup = make(map[string]string)
        //for k, v := range kv.kvStore {
        //    kv.kvStoreBackup[k] = v
        //}

        DPrintf("[kvserver: %v @ %v]Before delete sent shards, kvStore: %v\n", kv.me, kv.gid, kv.kvStore)
        // delete all the keys in args.ShardsList
        // get all the keys in map
        allKeys := make([]string, 0)
        for k, _ := range kv.kvStore {
            allKeys = append(allKeys, k)
        }
        for _, shards := range args.SendMap {
            // check the key is in args.ShardsList
            // if in, then delete from kv.kvStroe
            for _, shard := range shards {
                for _, key := range allKeys {
                    if key2shard(key) == shard {
                        delete(kv.kvStore, key)
                    }
                }
            }
        }
        DPrintf("[kvserver: %v @ %v]After delete sent shards, kvStore: %v\n", kv.me, kv.gid, kv.kvStore)

        args.Sent[0] = true
        
    case "MigrateShards":
        //if num, ok := kv.rcvdKVCmd[command.ClerkId]; ok && num == command.CommandNum {
        //    DPrintf("[kvserver: %v @ %v]%v, KVServer command %v is already committed.\n", kv.me, kv.gid, command.Name, command)
        //} else {
        args := command.Args.(MigrateShardsArgs)

        // copy the keys to kv.kvStore
        DPrintf("[kvserver: %v @ %v]Before migration, kvstore: %v\n", kv.me, kv.gid, kv.kvStore)
        for key, value := range args.KVPairs {
            // check the shard containing the key still in the kv.expectShardsList
            // at-most-once semantics
            for _, shard := range kv.expectShardsList {
                if key2shard(key) == shard {
                    kv.kvStore[key] = value
                }
            }
        }
        DPrintf("[kvserver: %v @ %v]After migration, new kvstore: %v\n", kv.me, kv.gid, kv.kvStore)

        // merge rcvdCmds
        DPrintf("[kvserver: %v @ %v]Before migration, rcvdCmd: %v\n", kv.me, kv.gid, kv.rcvdCmd)
        for srcClerkId, srcCmdId := range args.DupDtn {
            if tgtCmdId, ok := kv.rcvdCmd[srcClerkId]; ok {
                if srcCmdId > tgtCmdId {
                    kv.rcvdCmd[srcClerkId] = srcCmdId
                } 
            } else {
                kv.rcvdCmd[srcClerkId] = srcCmdId
            }
        }
        DPrintf("[kvserver: %v @ %v]After migration, rcvdCmd: %v\n", kv.me, kv.gid, kv.rcvdCmd)

        // remove the shards in args from kv.expectShardsList
        for _, shard := range args.ShardsList {
            matchIndex := -1
            for i, s := range kv.expectShardsList {
                if shard == s {
                    matchIndex = i
                    break
                }
            }
            if matchIndex != -1 {
                kv.expectShardsList = append(kv.expectShardsList[:matchIndex], kv.expectShardsList[matchIndex + 1:]...)
            }
            //DPrintf("[kvserver: %v @ %v]expectShardsList after remove: %v, logs: %v\n", kv.me, kv.gid, kv.expectShardsList, kv.rf.Logs)
        }
        DPrintf("[kvserver: %v @ %v]expectShardsList after remove: %v\n", kv.me, kv.gid, kv.expectShardsList)

        // if all the expected shards have been received
        // finish transition, set kv.inTransition to false
        if len(kv.expectShardsList) == 0 {
            kv.inTransition = false
        }

        //kv.rcvdKVCmd[command.ClerkId] = command.CommandNum
        //}
    }

    return value, Err
}

func (kv *ShardKV) checkLogSize(persister *raft.Persister, lastIndex int) {
    // detect when the persisted Raft state grows too large
    // hand a snapshot and tells Raft that it can discard old log entires
    // Raft should save with persist.SaveStateAndSnapshot()
    // kv server should restore the snapshot from the persister when it restarts
    if kv.maxraftstate != -1 && persister.RaftStateSize() > kv.maxraftstate {
        // snapshot
        kvState   := KvState{
            KvStore          : kv.kvStore,
            RcvdCmd          : kv.rcvdCmd,
            CurrentConfig    : kv.currentConfig,
            ExpectShardsList : kv.expectShardsList,
            InTransition     : kv.inTransition}
        snapshotStruct := raft.SnapshotData{
            KvState: kvState}

        buffer       := new(bytes.Buffer)
        e            := labgob.NewEncoder(buffer)
        //e.Encode(kv.kvStore)
        //e.Encode(kv.rcvdCmd)
        e.Encode(&snapshotStruct.KvState)
        snapshotData := buffer.Bytes()

        // send snapshot to raft 
        // tell it to discard logs and persist snapshot and remaining log
        kv.rf.CompactLog(snapshotData, lastIndex)
    }
}

func (kv *ShardKV) buildState(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    //kv.mu.Lock()
    //defer kv.mu.Unlock()
    snapshotData := raft.SnapshotData{}

    buffer := bytes.NewBuffer(data)
    d  := labgob.NewDecoder(buffer)

    if err := d.Decode(&snapshotData.KvState); err != nil {
        panic(err)
    }

    kvS        := snapshotData.KvState.(KvState)
    kv.kvStore          = kvS.KvStore
    kv.rcvdCmd          = kvS.RcvdCmd
    kv.currentConfig    = kvS.CurrentConfig
    kv.expectShardsList = kvS.ExpectShardsList
    kv.inTransition     = kvS.InTransition


    DPrintf("[kvserver: %v @ %v]After build kvServer state: kvstore: %v, kv.currentConfig: %v, kv.expectedShardsList: %v, kv.inTransition: %v\n", kv.me, kv.gid, kv.kvStore, kv.currentConfig, kv.expectShardsList, kv.inTransition)
}

func (kv *ShardKV) detectConfig(oldConfig shardmaster.Config, newConfig shardmaster.Config) (bool, map[int][]int, []int) {

    var isChanged          bool
    var sendMap            map[int][]int
    var expectShardsList   []int

    if oldConfig.Num == 0 && newConfig.Num == 1 {
        DPrintf("[kvserver: %v @ %v]initialize config\n", kv.me, kv.gid)
        isChanged = true
        sendMap   = nil
        expectShardsList = nil
        return isChanged, sendMap, expectShardsList
    }

    if oldConfig.Num > newConfig.Num {
        DPrintf("[kvserver: %v @ %v]old > new detectConfig, \nold: %v, \nnew: %v", kv.me, kv.gid, oldConfig, newConfig)
        panic("old config number should not greater than new config number")
    } else if oldConfig.Shards == newConfig.Shards {
        DPrintf("[kvserver: %v @ %v]NO config change\n", kv.me, kv.gid)
        isChanged        = false
        sendMap          = nil
        expectShardsList = nil
    } else {
        if oldConfig.Num != newConfig.Num - 1 {
            DPrintf("[kvserver: %v @ %v]Error: detectConfig, \nold: %v\nnew: %v\n", kv.me, kv.gid, oldConfig, newConfig)
            panic("Cannot handle non-consecutive config changes!")
        }
        DPrintf("[kvserver: %v @ %v]config changed\n", kv.me, kv.gid)
        isChanged = true
        for i := 0; i < shardmaster.NShards; i++ {
            // send map
            if kv.gid == oldConfig.Shards[i] && kv.gid != newConfig.Shards[i] {
                if sendMap == nil {
                    sendMap = make(map[int][]int)
                }
                targetGid          := newConfig.Shards[i]
                sendMap[targetGid]  = append(sendMap[targetGid], i)
                //DPrintf("sendMap:%v\n", sendMap)
            }

            // receive slice
            if kv.gid != oldConfig.Shards[i] && kv.gid == newConfig.Shards[i] {
                if expectShardsList == nil {
                    expectShardsList = make([]int, 0)
                }
                expectShardsList = append(expectShardsList, i)
                //DPrintf("expectShardsList:%v\n", expectShardsList)
            }
        }
    }
    DPrintf("[kvserver: %v @ %v]detectConfig, \nold: %v, \nnew: %v, \nsendMap: %v, \nexpectShardsList: %v\n", kv.me, kv.gid, oldConfig, newConfig, sendMap, expectShardsList)
    return isChanged, sendMap, expectShardsList
}

// only leader can all this function to send reconfig to raft
func (kv *ShardKV) reconfig(args *reconfigArgs) bool {

    DPrintf("[kvserver: %v @ %v]reconfig, args: %v\n", kv.me, kv.gid, args)
    rtnMsgCh := make(chan rtnMsg)
    
    kv.mu.Lock()

    // feed command to raft
    rfStateFeed := kv.feedCmd("reconfig", *args)
    if !rfStateFeed.isLeader {
        kv.mu.Unlock()
        return false
    }

    // add command channel to fifo
    kv.rtnChBuffer = append(kv.rtnChBuffer, rtnMsgCh)

    kv.mu.Unlock()

    rtnMsgApplied :=<- rtnMsgCh
    DPrintf("[kvserver: %v @ %v]reconfig, receive applied command\n", kv.me, kv.gid)

    sameState := kv.checkRfState(rfStateFeed, rtnMsgApplied.rfState)

    if !sameState {
        return false
    }


    return true
}


func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {

    rtnMsgCh := make(chan rtnMsg)

    kv.mu.Lock()

    // if an old Migrateshards received, 
    // means the sender didn't reply the finish reply from this group
    // reply OK
    if kv.currentConfig.Num > args.Num {
        reply.Err = OK
        kv.mu.Unlock()
        return
    }
    // if not in the transition, wait
    for kv.currentConfig.Num < args.Num {
        kv.mu.Unlock()
        time.Sleep(5 * time.Millisecond)
        kv.mu.Lock()
    }

    // feed command to raft
    rfStateFeed := kv.feedCmd("MigrateShards", *args)
    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    // add command channel to fifo
    kv.rtnChBuffer = append(kv.rtnChBuffer, rtnMsgCh)

    kv.mu.Unlock()

    rtnMsgApplied :=<- rtnMsgCh
    DPrintf("[kvserver: %v @ %v]MigrateShards, receive applied command\n", kv.me, kv.gid)

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rtnMsgApplied.rfState)

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
        return
    }

}

// function to send shards to other groups
func (kv *ShardKV) sendMigrateShards(tGid int, args *MigrateShardsArgs) {

    DPrintf("[kvserver: %v @ %v]sendMigrateShards args: %v", kv.me, kv.gid, args)
    
    servers := kv.currentConfig.Groups[tGid]
    // find all the key/value pairs related to the migration shards
    for {
        for si := 0; si < len(servers); si++ {
            srv   := kv.make_end(servers[si])
            reply := new(MigrateShardsReply)
            ok := srv.Call("ShardKV.MigrateShards", args, reply)
            if ok && reply.WrongLeader == false && reply.Err == OK {
                return
            }
            if ok && reply.Err == ErrWrongGroup {
                panic("unexpected ErrWrongGroup during reconfiguration")
            }
        }
    }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    defer DPrintf("[kvserver: %v @ %v]Get args: %v, reply: %v\n", kv.me, kv.gid, args, reply)

    rtnMsgCh := make(chan rtnMsg)

    kv.mu.Lock()

    // feed command to raft
    rfStateFeed := kv.feedCmd("Get", *args)
    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    // add command channel to fifo
    kv.rtnChBuffer = append(kv.rtnChBuffer, rtnMsgCh)
    kv.mu.Unlock()

    rtnMsgApplied :=<- rtnMsgCh
    DPrintf("[kvserver: %v @ %v]Get, receive applied command\n", kv.me, kv.gid)

    kv.mu.Lock()
    // if is in transition from one config to another
    // simply disallow all client operations
    //for !kv.isReady(args.Key) {
    //    kv.mu.Unlock()
    //    time.Sleep(50 * time.Millisecond)
    //    kv.mu.Lock()
    //}

    //// check is key in the correct group
    //if !kv.isInGroup(args.Key) {
    //    reply.Err = ErrWrongGroup
    //    kv.mu.Unlock()
    //    return
    //}

    if rtnMsgApplied.Err == ErrWrongGroup {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rtnMsgApplied.rfState)

    kv.mu.Unlock()

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
        return
    }

    // get value for read command
    //if value, exist := kv.kvStore[args.Key]; exist {
    //    reply.Err   = OK
    //    reply.Value = value
    //} else {
    //    reply.Err   = ErrNoKey
    //    reply.Value = ""
    //}
    reply.Err   = rtnMsgApplied.Err
    reply.Value = rtnMsgApplied.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    defer DPrintf("[kvserver: %v @ %v]PutAppend args: %v, reply: %v\n", kv.me, kv.gid, args, reply)

    rtnMsgCh := make(chan rtnMsg)

    kv.mu.Lock()

    // feed command to raft
    rfStateFeed := kv.feedCmd("PutAppend", *args)
    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    // add command channel to fifo
    kv.rtnChBuffer = append(kv.rtnChBuffer, rtnMsgCh)
    kv.mu.Unlock()

    rtnMsgApplied :=<- rtnMsgCh
    DPrintf("[kvserver: %v @ %v]PutAppend, receive applied command\n", kv.me, kv.gid)

    kv.mu.Lock()
    //// if is in transition from one config to another
    //// simply disallow all client operations
    //for !kv.isReady(args.Key) {
    //    kv.mu.Unlock()
    //    time.Sleep(50 * time.Millisecond)
    //    kv.mu.Lock()
    //}

    //// check is key in the correct group
    //if !kv.isInGroup(args.Key) {
    //    reply.Err = ErrWrongGroup
    //    kv.mu.Unlock()
    //    return
    //}

    if rtnMsgApplied.Err == ErrWrongGroup {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rtnMsgApplied.rfState)
    
    kv.mu.Unlock()

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
    }

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    close(kv.shutdown)
    DPrintf("[kvserver: %v @ %v]Kill kv server\n", kv.me, kv.gid)
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
    // register different command args struct
    labgob.Register(GetArgs{})
    labgob.Register(PutAppendArgs{})
    labgob.Register(reconfigArgs{})
    labgob.Register(MigrateShardsArgs{})
    labgob.Register(KvState{})

    // store in snapshot
    kv.kvStore         = make(map[string]string)
    kv.rcvdCmd         = make(map[int64]int)
    //kv.rcvdKVCmd       = make(map[int64]int)
    kv.commandNum      = 0

    //kv.kvStoreBackup     = nil

    kv.initialIndex    = 0
    kv.rtnChBuffer     = make([]chan rtnMsg, 0)
    kv.shutdown        = make(chan struct{})

	// Use something like this to talk to the shardmaster:
	kv.mck              = shardmaster.MakeClerk(kv.masters)
    kv.inTransition     = false
    kv.expectShardsList = nil

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    go func(kv *ShardKV) {
        for msg := range kv.applyCh {
            select {
            case <- kv.shutdown:
                return
            default:
                if msg.CommandValid { 
                    var rfStateApplied rfState
                    var rtnMsgApplied rtnMsg
                    var rtnMsgCh chan rtnMsg
                    applyIndex := msg.CommandIndex
                    kv.mu.Lock()
                    op  := msg.Command.(Op)
                    value, Err := kv.applyCmd(op)
                    kv.checkLogSize(persister, applyIndex)

                    // drain the rfState channel if not empty
                    if len(kv.rtnChBuffer) > 0  && kv.initialIndex <= applyIndex {
                        applyTerm, applyIsLeader := kv.rf.GetState()
                        rfStateApplied = rfState {
                            index    : applyIndex,
                            term     : applyTerm,
                            isLeader : applyIsLeader,
                            Err      : Err}
                        rtnMsgApplied = rtnMsg {
                            rfState : rfStateApplied,
                            Err     : Err,
                            Value   : value}
                        
                        rtnMsgCh, kv.rtnChBuffer = kv.rtnChBuffer[0], kv.rtnChBuffer[1:]
                        DPrintf("[kvserver: %v @ %v]drain the command to channel, %v\n", kv.me, kv.gid, rtnMsgApplied)
                        rtnMsgCh <- rtnMsgApplied
                    }
                    kv.mu.Unlock()
                } else {
                    kv.mu.Lock()
                    // InstallSnapshot RPC
                    DPrintf("[kvserver: %v @ %v]build state from InstallSnapShot: %v\n", kv.me, kv.gid, msg.Snapshot)
                    kv.buildState(msg.Snapshot)
                    kv.mu.Unlock()
                }
            }
        }
    }(kv)

    kv.buildState(persister.ReadSnapshot())

    // leader is responsible for polling config
    // use Sleep for 500 ms as a solution
    // a better solution could be condition variable Wait and Broadcast()
    go func(kv *ShardKV) {

        for {
            _, isLeader := kv.rf.GetState()
            if !isLeader {
		        time.Sleep(500 * time.Millisecond)
                continue
            } else {
                break
            }
        }
        // use a Get command to indicate log replay is done
        // don't care result
        getArgs := GetArgs{}
        getArgs.Key = "no-op"
        getReply := new(GetReply)
        kv.Get(&getArgs, getReply)

        for {
            select {
            case <-kv.shutdown:
                return
            default:
                _, isLeader := kv.rf.GetState()
                if !isLeader {
		            time.Sleep(500 * time.Millisecond)
                    continue
                }
                kv.mu.Lock()
                if kv.inTransition {
                    DPrintf("[kvserver: %v @ %v]inTransition state", kv.me, kv.gid)
                    kv.mu.Unlock()
                    time.Sleep(50 * time.Millisecond)
                    continue
                }
                DPrintf("[kvserver: %v @ %v]polling master for config, currentConfig: %v\n", kv.me, kv.gid, kv.currentConfig)
                currentConfig := kv.currentConfig
                queryNum      := kv.currentConfig.Num + 1
                kv.mu.Unlock()
                config := kv.mck.Query(queryNum)
                isChanged, sendMap, expectShardsList := kv.detectConfig(currentConfig, config)
                if isChanged {
                    // send reconfig to unerlying Raft
                    args                 := &reconfigArgs{}
                    args.Sent             = make([]bool, 1)
                    args.Sent[0]          = false
                    args.Config           = config
                    args.SendMap          = sendMap
                    args.ExpectShardsList = expectShardsList
                    //isSucceed := kv.reconfig(args)
                    // if return is false, meaning not the leader
                    // not need to do anything
                    kv.reconfig(args)

                    //if !isSucceed {
                    //    panic("reconfig failed")
                    //}
                }
		    }
		    time.Sleep(100 * time.Millisecond)
        }
    }(kv)

    DPrintf("[kvserver: %v @ %v]Start!\n", kv.me, kv.gid)
	return kv
}
