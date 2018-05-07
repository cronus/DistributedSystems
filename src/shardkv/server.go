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

    // need to initial when is the Leader
    initialIndex    int
    rfStateChBuffer []chan rfState

    // for shutdown
    shutdown chan struct{}
}

type rfState struct {
    index    int
    term     int
    isLeader bool
}

// function to check a key is ready during transition
// TODO more fine check, 
//     1. to allow irrelevant key to proceceed
//     2. to allow received key to proceed
func (kv *ShardKV) isReady(key string) bool {
    var ready bool
    if !kv.inTransition {
        ready = true
    } else {
        ready = false
    }

    DPrintf("[kvserver: %v @ %v]check key: %v is ready: %v\n", kv.me, kv.gid, key, ready)
    return ready
}

// function to check key in group
func (kv *ShardKV) isInGroup(key string) bool {
    keyShardIndex := key2shard(key)
    return kv.gid == kv.currentConfig.Shards[keyShardIndex]
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
        clerkId    = 0
        commandNum = -1
    case "MigrateShards":
        clerkId    = -1
        commandNum = -1
        
    default:
        panic("unexpected command!")
    }

    op := Op{
        Name       : name,
        Args       : args,
        ClerkId    : clerkId,
        CommandNum : commandNum}

    index, term, isLeader := kv.rf.Start(op)

    if len(kv.rfStateChBuffer) == 0 {
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

func (kv *ShardKV) applyCmd(command Op) {

    DPrintf("[kvserver: %v @ %v]Apply command: %v", kv.me, kv.gid, command)

    // duplicated command detection
    if num, ok := kv.rcvdCmd[command.ClerkId]; ok && num == command.CommandNum {
        DPrintf("[kvserver: %v @ %v]%v, command %v is already committed.\n", kv.me, kv.gid, command.Name, command)
    } else {
        switch command.Name {
        case "PutAppend":
            args := command.Args.(PutAppendArgs)

            switch args.Op {
            case "Put":
                kv.kvStore[args.Key]  = args.Value
            case "Append":
                kv.kvStore[args.Key] += args.Value
            }

        case "Get":
            // no status change for Get command
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
        
    case "MigrateShards":
        args := command.Args.(MigrateShardsArgs)

        // copy the keys to kv.kvStore
        DPrintf("[kvserver: %v @ %v]Before migration, kvstore: %v\n", kv.me, kv.gid, kv.kvStore)
        for key, value := range args.KVPairs {
            kv.kvStore[key] = value
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
    }
}

func (kv *ShardKV) checkLogSize(persister *raft.Persister, lastIndex int) {
    // detect when the persisted Raft state grows too large
    // hand a snapshot and tells Raft that it can discard old log entires
    // Raft should save with persist.SaveStateAndSnapshot()
    // kv server should restore the snapshot from the persister when it restarts
    if kv.maxraftstate != -1 && persister.RaftStateSize() > kv.maxraftstate {
        // snapshot
        buffer       := new(bytes.Buffer)
        e            := labgob.NewEncoder(buffer)
        e.Encode(kv.kvStore)
        e.Encode(kv.rcvdCmd)
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

    buffer := bytes.NewBuffer(data)
    d  := labgob.NewDecoder(buffer)

    if err := d.Decode(&kv.kvStore); err != nil {
        panic(err)
    }

    // restore receivedCmd
    if err := d.Decode(&kv.rcvdCmd); err != nil {
        panic(err)
    }


    DPrintf("[kvserver: %v @ %v]After build kvServer state: kvstore: %v\n", kv.me, kv.gid, kv.kvStore)
}

func (kv *ShardKV) detectConfig(oldConfig shardmaster.Config, newConfig shardmaster.Config) (bool, map[int][]int, []int) {

    var isChanged          bool
    var sendMap            map[int][]int
    var expectShardsList   []int

    if oldConfig.Num > newConfig.Num {
        panic("old config number should not greater than new config number")
    } else if oldConfig.Shards == newConfig.Shards {
        DPrintf("[kvserver: %v @ %v]config no change\n", kv.me, kv.gid)
        isChanged        = false
        sendMap          = nil
        expectShardsList = nil
    } else {
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
    DPrintf("[kvserver: %v @ %v]detectConfig, old: %v, new: %v, sendMap: %v, expectShardsList: %v\n", kv.me, kv.gid, oldConfig, newConfig, sendMap, expectShardsList)
    return isChanged, sendMap, expectShardsList
}

// only leader can all this function to send reconfig to raft
func (kv *ShardKV) reconfig(args *reconfigArgs, sendMap map[int][]int) bool {

    DPrintf("[kvserver: %v @ %v]reconfig, args: %v, sendMap: %v\n", kv.me, kv.gid, args, sendMap)
    rfStateCh := make(chan rfState)
    
    kv.mu.Lock()

    // feed command to raft
    rfStateFeed := kv.feedCmd("reconfig", *args)
    if !rfStateFeed.isLeader {
        kv.mu.Unlock()
        return false
    }

    // add command channel to fifo
    kv.rfStateChBuffer = append(kv.rfStateChBuffer, rfStateCh)
    kv.mu.Unlock()

    rfStateApplied :=<- rfStateCh
    DPrintf("[kvserver: %v @ %v]reconfig, receive applied command\n", kv.me, kv.gid)

    sameState := kv.checkRfState(rfStateFeed, rfStateApplied)

    if !sameState {
        return false
    }

    kv.mu.Lock()
    // send to other groups
    // TODO remove the key/value pairs
    for gid, shards := range sendMap {

        kvPairs := make(map[string]string)
        // find all the keys related to the gid and sendMap[gid]
        for _, shard := range shards {
            for key, value := range kv.kvStore {
                if shard == key2shard(key) {
                    kvPairs[key] = value
                }
            }
        }

        args           := &MigrateShardsArgs{}
        args.ShardsList = shards
        args.KVPairs    = kvPairs
        args.DupDtn     = kv.rcvdCmd

        DPrintf("[kvserver: %v @ %v]args send to other groups: %v\n", kv.me, kv.gid, args)

        //reply := new(MigrateShardsReply)

        go kv.sendMigrateShards(gid, args)
    }

    kv.mu.Unlock()

    return true
}


func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {

    rfStateCh := make(chan rfState)

    kv.mu.Lock()

    // if is not in transition from one config to another
    // disallow shards migration operations
    for !kv.inTransition {
        kv.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
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
    kv.rfStateChBuffer = append(kv.rfStateChBuffer, rfStateCh)
    kv.mu.Unlock()

    rfStateApplied :=<- rfStateCh
    DPrintf("[kvserver: %v @ %v]MigrateShards, receive applied command\n", kv.me, kv.gid)

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rfStateApplied)

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
        return
    }

}

// function to send shards to other groups
func (kv *ShardKV) sendMigrateShards(tGid int, args *MigrateShardsArgs) {

    DPrintf("[kvserver: %v @ %v]sendMirgareShards args: %v", kv.me, kv.gid, args)
    
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

    rfStateCh := make(chan rfState)

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
    kv.rfStateChBuffer = append(kv.rfStateChBuffer, rfStateCh)
    kv.mu.Unlock()

    rfStateApplied :=<- rfStateCh
    DPrintf("[kvserver: %v @ %v]Get, receive applied command\n", kv.me, kv.gid)

    kv.mu.Lock()
    // if is in transition from one config to another
    // simply disallow all client operations
    for !kv.isReady(args.Key) {
        kv.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
        kv.mu.Lock()
    }

    // check is key in the correct group
    if !kv.isInGroup(args.Key) {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rfStateApplied)

    kv.mu.Unlock()

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
        return
    }

    // get value for read command
    if value, exist := kv.kvStore[args.Key]; exist {
        reply.Err   = OK
        reply.Value = value
    } else {
        reply.Err   = ErrNoKey
        reply.Value = ""
    }
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    defer DPrintf("[kvserver: %v @ %v]PutAppend args: %v, reply: %v\n", kv.me, kv.gid, args, reply)

    rfStateCh := make(chan rfState)

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
    kv.rfStateChBuffer = append(kv.rfStateChBuffer, rfStateCh)
    kv.mu.Unlock()

    rfStateApplied :=<- rfStateCh
    DPrintf("[kvserver: %v @ %v]PutAppend, receive applied command\n", kv.me, kv.gid)

    kv.mu.Lock()
    // if is in transition from one config to another
    // simply disallow all client operations
    for !kv.isReady(args.Key) {
        kv.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
        kv.mu.Lock()
    }

    // check is key in the correct group
    if !kv.isInGroup(args.Key) {
        reply.Err = ErrWrongGroup
        kv.mu.Unlock()
        return
    }

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rfStateApplied)
    
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

    // store in snapshot
    kv.rcvdCmd         = make(map[int64]int)
    kv.kvStore         = make(map[string]string)

    kv.initialIndex    = 0
    kv.rfStateChBuffer = make([]chan rfState, 0)
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
                    var rfStateCh chan rfState
                    applyIndex := msg.CommandIndex
                    kv.mu.Lock()
                    op := msg.Command.(Op)
                    kv.applyCmd(op)
                    kv.checkLogSize(persister, applyIndex)

                    // drain the rfState channel if not empty
                    if len(kv.rfStateChBuffer) > 0  && kv.initialIndex <= applyIndex {
                        applyTerm, applyIsLeader := kv.rf.GetState()
                        rfStateApplied = rfState {
                            index    : applyIndex,
                            term     : applyTerm,
                            isLeader : applyIsLeader}
                        
                        rfStateCh, kv.rfStateChBuffer = kv.rfStateChBuffer[0], kv.rfStateChBuffer[1:]
                        DPrintf("[kvserver: %v @ %v]drain the command to channel, %v\n", kv.me, kv.gid, rfStateApplied)
                        rfStateCh <- rfStateApplied
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

    // leader is responsible for polling config
    // use Sleep for 500 ms as a solution
    // a better solution could be condition variable Wait and Broadcast()
    go func(kv *ShardKV) {
        // initial kv.currentConfig
        var initConfig shardmaster.Config
        for initConfig = kv.mck.Query(-1); initConfig.Num == 0; {
            initConfig = kv.mck.Query(-1)
        }
        DPrintf("[kvserver: %v @ %v]initialize config: %v\n", kv.me, kv.gid, initConfig)
        kv.mu.Lock()
        kv.currentConfig = initConfig
        kv.mu.Unlock()
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
                config := kv.mck.Query(-1)
                isChanged, sendMap, expectShardsList := kv.detectConfig(kv.currentConfig, config)
                if isChanged {
                    // send reconfig to unerlying Raft
                    args                 := &reconfigArgs{}
                    args.Config           = config
                    args.ExpectShardsList = expectShardsList
                    isSucceed := kv.reconfig(args, sendMap)

                    if !isSucceed {
                        panic("reconfig failed")
                    }
                }
		        time.Sleep(100 * time.Millisecond)
		    }
        }
    }(kv)

	return kv
}
