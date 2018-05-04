package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "log"
import "bytes"

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

    defer DPrintf("[kvserver: %v]%v, args: %v state: %v\n", kv.me, name, args, state)
    return state

}

func (kv *ShardKV) checkRfState(oldState rfState, newState rfState) bool {

    var sameState bool

    if oldState.index != newState.index {
        DPrintf("[kvserver: %v]oldState: %v, newState: %v", kv.me, oldState, newState)
        panic("index not equal!")
    } else if oldState.isLeader != newState.isLeader {
        DPrintf("[kvserver: %v]not the leader\n", kv.me)
        sameState = false
    } else if oldState.term != newState.term {
        DPrintf("[kvserver: %v]term differ\n", kv.me)
        sameState = false
    } else {
        sameState = true
    }

    return sameState
}

func (kv *ShardKV) applyCmd(command Op) {

    DPrintf("[kvserver: %v]Apply command: %v", kv.me, command)

    // duplicated command detection
    if num, ok := kv.rcvdCmd[command.ClerkId]; ok && num == command.CommandNum {
        DPrintf("[kvserver: %v]%v, command %v is already committed.\n", kv.me, command.Name, command)
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


    DPrintf("[kvserver: %v]After build kvServer state: kvstore: %v\n", kv.me, kv.kvStore)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    defer DPrintf("[kvserver: %v]Get args: %v, reply: %v\n", kv.me, args, reply)

    rfStateCh := make(chan rfState)

    kv.mu.Lock()
    rfStateFeed := kv.feedCmd("Get", *args)

    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    kv.rfStateChBuffer = append(kv.rfStateChBuffer, rfStateCh)
    kv.mu.Unlock()

    rfStateApplied :=<- rfStateCh
    DPrintf("[kvserver: %v]Get, receive applied command\n", kv.me)

    // handle reply
    sameState := kv.checkRfState(rfStateFeed, rfStateApplied)

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
        return
    }

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
    defer DPrintf("[kvserver: %v]PutAppend args: %v, reply: %v\n", kv.me, args, reply)

    rfStateCh := make(chan rfState)

    kv.mu.Lock()
    rfStateFeed := kv.feedCmd("PutAppend", *args)

    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    kv.rfStateChBuffer = append(kv.rfStateChBuffer, rfStateCh)
    kv.mu.Unlock()

    rfStateApplied :=<- rfStateCh
    DPrintf("[kvserver: %v]PutAppend, receive applied command\n", kv.me)

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
    DPrintf("[kvserver: %v]Kill kv server\n", kv.me)
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
                        DPrintf("[kvserver: %v]drain the command to channel, %v\n", kv.me, rfStateApplied)
                        rfStateCh <- rfStateApplied
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
        }
    }(kv)

    // poll config
    go func(kv *ShardKV) {
        for {
            select {
            case <-kv.shutdown:
                return
            default:
                // try each known server.
		        for _, srv := range kv.masters {
			        var reply QueryReply
			        ok := srv.Call("ShardMaster.Query", args, &reply)
			        if ok && reply.WrongLeader == false {
				        kv.currentConfig = reply.Config
			        }
                }
		        time.Sleep(100 * time.Millisecond)
		    }
        }
    }

	return kv
}
