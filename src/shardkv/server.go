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

    mck *shardmaster.Clerk
    currentConfig shardmaster.Config

    // persist when snapshot
    kvStore map[string]string
    rcvdCmd map[int64]int

    // need to initial when becoming Leader
    initialIndex int
    rfStateChBuffer []chan rfState

    shutdown chan struct{}
}

type rfState struct {
    index    int
    term     int
    isLeader bool
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
        case "reconfig":

            kv.currentConfig = command.Args.(shardmaster.Config)
            kv.inTransition  = true
            DPrintf("[kvserver: %v @ %v]update config: %v", kv.me, kv.gid, kv.currentConfig)
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

func (kv *ShardKV) detectConfig(oldConfig shardmaster.Config, newConfig shardmaster.Config) (map[int][]string, []int) {

    var sendMap         map[int][]string
    var rcvShardsList   []int

    if oldConfig.Num > newConfig.Num {
        panic("old config number should not greater than new config number")
    } else if oldConfig.Shards == newConfig.Shards {
        DPrintf("[kvserver: %v @ %v]config no change\n", kv.me, kv.gid)
        sendMap   = nil
        rcvShardsList = nil
    } else {
        DPrintf("[kvserver: %v @ %v]config changed\n", kv.me, kv.gid)
        for i := 0; i < shardmaster.NShards; i++ {
            // send map
            if kv.gid == oldConfig.Shards[i] && kv.gid != newConfig.Shards[i] {
                if sendMap == nil {
                    sendMap = make(map[int][]string)
                }
                targetGid  := newConfig.Shards[i]
                sendMap[i]  = newConfig.Groups[targetGid]
                //DPrintf("sendMap:%v\n", sendMap)
            }

            // receive slice
            if kv.gid != oldConfig.Shards[i] && kv.gid == newConfig.Shards[i] {
                if rcvShardsList == nil {
                    rcvShardsList = make([]int, 0)
                }
                rcvShardsList = append(rcvShardsList, i)
                //DPrintf("rcvShardsList:%v\n", rcvShardsList)
            }
        }
    }
    DPrintf("[kvserver: %v @ %v]detectConfig, old: %v, new: %v, sendMap: %v, rcvList: %v\n", kv.me, kv.gid, oldConfig, newConfig, sendMap, rcvShardsList)
    return sendMap, rcvShardsList
}

// only leader can all this function to send reconfig to raft
func (kv *ShardKV) reconfig(args *shardmaster.Config, sendMap map[int][]string, rcvShardsList []int) bool {

    DPrintf("[kvserver: %v @ %v]reconfig, args: %v, sendMap: %v, rcvShardsList: %v\n", kv.me, kv.gid, args, sendMap, rcvShardsList)
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
    kv.sendShards(sendMap)

    // waiting to receive
    kv.rcvShards(rcvShardsList) 
    kv.mu.Unlock()

    return true
}


func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {
}

// function to send shards to other groups
func (kv *ShardKV) sendMigrateShards(tGid int, args *MigrateShardsArgs, reply *MigrateStardsReply, sendMap map[int][]string) {

    DPrintf("[kvserver: %v @ %v]sendShards: %v", kv.me, kv.gid, sendMap)
    

    // find all the key/value pairs related to the migration shards
    for key, _ := range kv.kvStore {

        shard := key2shard(key)

        // check if the key ever been Put/Append in kvStore
        _, keyInKvStore := kv.kvStore[key]
        if servers, ok := sendMap[shard]; ok {
	        args := MigrateShardsArgs{}
	        args.Maps
            for si := 0; si < len(servers); si++ {
                srv := kv.make_end(servers[si])
                var reply PutAppendReply
                ok := srv.Call("ShardKV.MigrateShards", &args, &reply)
                if ok && reply.WrongLeader == false && reply.Err == OK {
                    return
                }
                if ok && reply.Err == ErrWrongGroup {
                    panic("unexpected ErrWrongGroup during reconfiguration")
                }
            }
        }
    }
}

// function to receive shards from other groups
func (kv *ShardKV) rcvShards(rcvList []int) {
    DPrintf("[kvserver: %v @ %v]rcvShards: %v", kv.me, kv.gid, rcvList)

    if len(rcvList) == 0 {
        return
    }

    for rcvlen := len(rcvList); rcvlen != 0 {
        Wait()
    }

    return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    defer DPrintf("[kvserver: %v @ %v]Get args: %v, reply: %v\n", kv.me, args, reply, kv.gid)

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

    // check is key in the correct group
    if !kv.isInGroup(args.Key) {
        reply.Err = ErrWrongGroup
        return
    }

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rfStateApplied)

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

    // check is key in the correct group
    if !kv.isInGroup(args.Key) {
        reply.Err = ErrWrongGroup
        return
    }
    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rfStateApplied)

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
    labgob.Register(shardmaster.Config{})

    // store in snapshot
    kv.rcvdCmd         = make(map[int64]int)
    kv.kvStore         = make(map[string]string)

    kv.initialIndex    = 0
    kv.rfStateChBuffer = make([]chan rfState, 0)
    kv.shutdown        = make(chan struct{})

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

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
                sendMap, rcvShardsList := kv.detectConfig(kv.currentConfig, config)
                if len(sendMap) != 0 || len(rcvShardsList) != 0 {
                    // send reconfig to unerlying Raft
                    isSucceed := kv.reconfig(&config, sendMap, rcvShardsList)

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
