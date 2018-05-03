package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

    cmdChBuffer []chan bool

    // map for duplicated command detection
    rcvdCmd map[int64]int


    shutdown chan interface{}
}


type Op struct {
	// Your data here.
    Name string
    Args interface{}
}

func (sm *ShardMaster) feedCmd(name string, args interface{}) bool {
    DPrintf("[smserver: %v]%v, args: %v\n", sm.me, name, args)

    op := Op{
        Name   : name,
        Args   : args}

    index, term, isLeader := sm.rf.Start(op)

    return isLeader

}

func (sm *ShardMaster) applyCmd(command Op) {

    DPrintf("[smserver: %v]Apply command: %v", sm.me, command)

    // duplicated command detection
    if num, ok := sm.rcvdCmd[command.Args.ClerkId]; ok && num == command.Args.CommandNum {
        DPrintf("[smserver: %v]%v, command %v is already committed.\n", sm.me, command.Name, command)
    } else {
        switch Op.Name {
        case "Join":
            // creating a new configuration that includes the new replica groups.
            // The new configuration should divide the shards as evenly as possible
            // among the full set of groups, and should move as few shards as possible 
            // Note: should allow re-use of a GID if it's not part of the current configuration

            // init
            newGroups := make(map[int][]string)
            for gid, ss := range command.Args.Groups {
                newGroups[gid] = ss
            }
            newShards := sm.configs[len(sm.configs) - 1].Shards

            // check GID is not used
            // add the new GID to Groups
            for gid, ss := range command.Args.Servers {
                if _, ok := Op.Args.Groups[k]; ok{
                    panic("Join: current state contains gid")
                } else {
                    newGroups[gid] = ss
                }
            }

            // TODO re-allocate shards

            // create a new config, append to configs
            newConfig := Config {
                Num    : len(sm.configs),
                Shards : newShards,
                Groups : newGroups}
            }
            configs = append(configs, newConfig)

        case "Leave":
            // creating a new configuration that dows not include those groups
            // assign those groups' shards to the remaining groups
            // The new configuration should divide the shards as evenly as possible
            // among the full set of groups, and should move as few shards as possible 

            // init
            newGroups := make(map[int][]string)
            for gid, ss := range Op.Args.Groups {
                newGroups[gid] = ss
            }
            newShards := sm.configs[len(sm.configs) - 1].Shards

            // delete the Leave groups
            for _, gid := range command.Args.GIDs {
                delete(newGroup, gid)
            }

            // TODO re-allocate shards
            
            // create a new config, append to config
            newConfig = Config{
                Num    : len(sm.configs),
                Shards : newShards,
                Groups : newGroup}

            configs = append(configs, newConfig)

        case "Move":
            // creating a new configuration in which the shard is assigned to the group

            // init
            newGroups := make(map[int][]string)
            for gid, ss := range command.Args.Groups {
                newGroups[gid] = ss
            }
            newShards := sm.configs[len(sm.configs) - 1].Shards

            // shards is assigned to the group
            newShards[Shard] = GID

            // create a new config, append to config
            newConfig = Config{
                Num    : len(sm.configs),
                Shards : newShards,
                Groups : newGroup}
                
            configs = append(configs, newConfig)

        case "Query":
            // replying with configuration that has the queried number
            // if the number is -1 or bigger than the biggest known configuration number,
            // the shardmaster should reply with the latest configuration.
            // The result of Query(-1) should reflect every Join, Leave or Move RPC that 
            // the shardmaster finished handling before it recevied the Query(-1) RPC

            if command.Args.Num == -1 or command.Args.Num > len(sm.configs) - 1 {
                qCfgIndex = len(sm.config) - 1
            } else {
                qCfgIndex = command.Args.Num
            }

            sm.configs[qCfgIndex]

        default:
            panic("Unknown Command!")
        }
    }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
    sm.mu.Lock()
    feedCmd("Join", args)
    sm.mu.Unlock()
    <- sm.cmdChBuffer[0]
    // handle reply


}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
    sm.mu.Lock()
    defer sm.mu.Unlock()

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
    sm.mu.Lock()
    defer sm.mu.Unlock()

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
    sm.mu.Lock()
    defer sm.mu.Unlock()

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

    // The very first configuration should be numbered zero
    // all shards should be assigned go GID 0
    // although the default values are satisfied, set explicitly
    sm.configs[0].Num = 0
    for _, shard := range sm.config[0].Shards {
        shard = 0
    }
    

    go func(sm *SMServer) {
        for msg := range sm.applyCh {
            select {
            case <- sm.shutdown:
                return
            default:
                sm.mu.Lock()
                sm.applyCmd()
                sm.mu.Unlock()
            }
        }
    }

	return sm
}
