package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "log"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

    // map for duplicated command detection
    rcvdCmd map[int64]int

    initialIndex int
    rfStateChBuffer []chan rfState // true: leader; false: non-leader

    shutdown chan struct{}
}


type Op struct {
	// Your data here.
    Name       string
    Args       interface{}
    ClerkId    int64
    CommandNum int
}

type rfState struct {
    index    int
    term     int
    isLeader bool
}

// function to feed command to raft
func (sm *ShardMaster) feedCmd(name string, args interface{}) rfState {

    var clerkId    int64
    var commandNum int
    var state rfState

    switch name {
    case "Join":
        a         := args.(JoinArgs)
        clerkId    = a.ClerkId
        commandNum = a.CommandNum
    case "Leave":
        a         := args.(LeaveArgs)
        clerkId    = a.ClerkId
        commandNum = a.CommandNum
    case "Move":
        a         := args.(MoveArgs)
        clerkId    = a.ClerkId
        commandNum = a.CommandNum
    case "Query":
        a         := args.(QueryArgs)
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

    index, term, isLeader := sm.rf.Start(op)

    if len(sm.rfStateChBuffer) == 0 {
        sm.initialIndex = index
    }

    state = rfState {
        index    : index,
        term     : term,
        isLeader : isLeader}

    defer DPrintf("[smserver: %v]%v, args: %v state: %v\n", sm.me, name, args, state)
    return state

}

func (sm *ShardMaster) checkRfState(oldState rfState, newState rfState) bool {

    var sameState bool

    if oldState.index != newState.index {
        DPrintf("[smserver: %v]oldState: %v, newState: %v", sm.me, oldState, newState)
        panic("index not equal!")
    } else if oldState.isLeader != newState.isLeader {
        DPrintf("[smserver: %v]not the leader\n", sm.me)
        sameState = false
    } else if oldState.term != newState.term {
        DPrintf("[smserver: %v]term differ\n", sm.me)
        sameState = false
    } else {
        sameState = true
    }

    return sameState
}

func (sm *ShardMaster) applyCmd(command Op) {

    DPrintf("[smserver: %v]Apply command: %v", sm.me, command)

    // duplicated command detection
    if num, ok := sm.rcvdCmd[command.ClerkId]; ok && num == command.CommandNum {
        DPrintf("[smserver: %v]%v, command %v is already committed.\n", sm.me, command.Name, command)
    } else {
        switch command.Name {
        case "Join":
            // creating a new configuration that includes the new replica groups.
            // The new configuration should divide the shards as evenly as possible
            // among the full set of groups, and should move as few shards as possible 
            // Note: should allow re-use of a GID if it's not part of the current configuration

            // init
            args      := command.Args.(JoinArgs)
            keys      := make([]int, 0)
            newGroups := make(map[int][]string)
            for gid, ss := range sm.configs[len(sm.configs) - 1].Groups {
                newGroups[gid] = ss
                keys = append(keys, gid)
            }
            newShards := sm.configs[len(sm.configs) - 1].Shards

            // check GID is not used
            // add the new GID to Groups
            for gid, ss := range args.Servers {
                if _, ok := newGroups[gid]; ok{
                    panic("Join: current state contains gid")
                } else {
                    newGroups[gid] = ss
                    keys           = append(keys, gid)
                }
            }

            DPrintf("[smserver: %v]Join, keys of config.Group: %v\n", sm.me, keys)
            // re-allocate shards, consistent hashing
            // assume every group has 10 virtual node
            for i, _ := range newShards {
                gidIndex := i % len(keys) 
                newShards[i] = keys[gidIndex]
            }

            // create a new config, append to configs
            newConfig := Config {
                Num    : len(sm.configs),
                Shards : newShards,
                Groups : newGroups}

            sm.configs = append(sm.configs, newConfig)

        case "Leave":
            // creating a new configuration that dows not include those groups
            // assign those groups' shards to the remaining groups
            // The new configuration should divide the shards as evenly as possible
            // among the full set of groups, and should move as few shards as possible 

            // init
            args      := command.Args.(LeaveArgs)
            keys      := make([]int, 0)
            newGroups := make(map[int][]string)
            for gid, ss := range sm.configs[len(sm.configs) - 1].Groups {
                newGroups[gid] = ss
            }
            newShards := sm.configs[len(sm.configs) - 1].Shards

            // delete the Leave groups
            for _, gid := range args.GIDs {
                if _, ok := newGroups[gid]; ok {
                    delete(newGroups, gid)
                } else {
                    panic("Leave: current state doesn't contain gid")
                }
            }

            DPrintf("[smserver: %v]Leave, keys of config.Group: %v\n", sm.me, keys)
            for gid, _ := range newGroups {
                keys = append(keys, gid)
            }

            // re-allocate shards
            if len(keys) != 0 {
                for i, _ := range newShards {
                    gidIndex := i % len(keys) 
                    newShards[i] = keys[gidIndex]
                }
            } else {
                for i, _ := range newShards {
                    newShards[i] = 0
                }
            }
            
            // create a new config, append to config
            newConfig := Config{
                Num    : len(sm.configs),
                Shards : newShards,
                Groups : newGroups}

            sm.configs = append(sm.configs, newConfig)

        case "Move":
            // creating a new configuration in which the shard is assigned to the group

            // init
            args      := command.Args.(MoveArgs)
            newGroups := make(map[int][]string)
            for gid, ss := range sm.configs[len(sm.configs) - 1].Groups {
                newGroups[gid] = ss
            }
            newShards := sm.configs[len(sm.configs) - 1].Shards

            // shards is assigned to the group
            newShards[args.Shard] = args.GID

            // create a new config, append to config
            newConfig := Config{
                Num    : len(sm.configs),
                Shards : newShards,
                Groups : newGroups}
                
            sm.configs = append(sm.configs, newConfig)

        case "Query":

            // read command is handled at command function

        default:
            panic("Unknown Command!")
        }
    }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

    defer DPrintf("[smserver: %v]Join args: %v, reply: %v\n", sm.me, args, reply)

    rfStateCh := make(chan rfState)

    sm.mu.Lock()
    rfStateFeed := sm.feedCmd("Join", *args)

    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        sm.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    sm.rfStateChBuffer = append(sm.rfStateChBuffer, rfStateCh)
    sm.mu.Unlock()
   
    rfStateApplied :=<- rfStateCh
    DPrintf("[smserver: %v]Join, receive applied command\n", sm.me)

    // handle reply
    sameState := sm.checkRfState(rfStateFeed, rfStateApplied)

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
    }

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

    defer DPrintf("[smserver: %v]Leave args: %v, reply: %v\n", sm.me, args, reply)
    
    rfStateCh := make(chan rfState)

    sm.mu.Lock()
    rfStateFeed := sm.feedCmd("Leave", *args)

    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        sm.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    sm.rfStateChBuffer = append(sm.rfStateChBuffer, rfStateCh)
    sm.mu.Unlock()
   
    rfStateApplied :=<- rfStateCh
    DPrintf("[smserver: %v]Leave, receive applied command\n", sm.me)

    // handle reply
    sameState := sm.checkRfState(rfStateFeed, rfStateApplied)

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
    }

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

    DPrintf("[smserver: %v]Move args: %v, reply: %v\n", sm.me, args, reply)

    rfStateCh := make(chan rfState)

    sm.mu.Lock()
    rfStateFeed := sm.feedCmd("Move", *args)

    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        sm.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    sm.rfStateChBuffer = append(sm.rfStateChBuffer, rfStateCh)
    sm.mu.Unlock()
   
    rfStateApplied :=<- rfStateCh
    DPrintf("[smserver: %v]Move, receive applied command\n", sm.me)

    // handle reply
    sameState := sm.checkRfState(rfStateFeed, rfStateApplied)

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
    }

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

    defer DPrintf("[smserver: %v]Query args: %v, reply: %v\n", sm.me, args, reply)

    rfStateCh := make(chan rfState)

    sm.mu.Lock()
    rfStateFeed := sm.feedCmd("Query", *args)

    if !rfStateFeed.isLeader {
        reply.WrongLeader = true
        sm.mu.Unlock()
        return
    } else {
        reply.WrongLeader = false
    }

    sm.rfStateChBuffer = append(sm.rfStateChBuffer, rfStateCh)
    sm.mu.Unlock()
   
    rfStateApplied :=<- rfStateCh
    DPrintf("[smserver: %v]Query, receive applied command\n", sm.me)

    // handle reply
    sameState := sm.checkRfState(rfStateFeed, rfStateApplied)

    if sameState {
        reply.Err = OK
    } else {
        reply.Err = ErrLeaderChanged
        return
    }

    // replying with configuration that has the queried number
    // if the number is -1 or bigger than the biggest known configuration number,
    // the shardmaster should reply with the latest configuration.
    // The result of Query(-1) should reflect every Join, Leave or Move RPC that 
    // the shardmaster finished handling before it recevied the Query(-1) RPC
    sm.mu.Lock()
    var qCfgIndex int

    if args.Num == -1 || args.Num > len(sm.configs) - 1 {
        qCfgIndex = len(sm.configs) - 1
    } else {
        qCfgIndex = args.Num
    }

    reply.Config = sm.configs[qCfgIndex]
    sm.mu.Unlock()

}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.

    sm.mu.Lock()
    defer sm.mu.Unlock()

    close(sm.shutdown)

    DPrintf("[smserver: %v]Kill sm server\n", sm.me)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

    // register different command args struct
    labgob.Register(JoinArgs{})
    labgob.Register(LeaveArgs{})
    labgob.Register(MoveArgs{})
    labgob.Register(QueryArgs{})

    // The very first configuration should be numbered zero
    // all shards should be assigned go GID 0
    // although the default values are satisfied, set explicitly
    sm.configs[0].Num = 0
    for i, _ := range sm.configs[0].Shards {
        sm.configs[0].Shards[i] = 0
    }

    sm.initialIndex    = 0
    sm.rfStateChBuffer = make([]chan rfState, 0)
    sm.shutdown        = make(chan struct{})

    go func(sm *ShardMaster) {
        for msg := range sm.applyCh {
            select {
            case <- sm.shutdown:
                return
            default:
                var rfStateApplied rfState
                var rfStateCh chan rfState
                sm.mu.Lock()
                op := msg.Command.(Op)
                sm.applyCmd(op)

                applyIndex               := msg.CommandIndex
                // drain the rfState channel if not empty
                if len(sm.rfStateChBuffer) > 0  && sm.initialIndex <= applyIndex {
                    applyTerm, applyIsLeader := sm.rf.GetState()
                    rfStateApplied = rfState {
                        index    : applyIndex,
                        term     : applyTerm,
                        isLeader : applyIsLeader}
                    
                    rfStateCh, sm.rfStateChBuffer = sm.rfStateChBuffer[0], sm.rfStateChBuffer[1:]
                    DPrintf("[smserver: %v]drain the command to channel, %v\n", sm.me, rfStateApplied)
                    rfStateCh <- rfStateApplied
                }
                sm.mu.Unlock()
            }
        }
    }(sm)

	return sm
}
