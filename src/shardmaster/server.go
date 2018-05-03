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


    rcvdCmd map[int64]int


    shutdown chan interface{}
}


type Op struct {
	// Your data here.
    Name string

}

func (sm *ShardMaster) dispatchCmd(name string, args interface{}) {
    DPrintf("[smserver: %v]%v, args: %v\n", sm.me, name, args)
}

func (sm *ShardMaster) handleCmd(command Op) {

    DPrintf("[smserver: %v]Handle command: %v", sm.me, command)

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

            config := make(Config)

        case "Leave":
            // creating a new configuration that dows not include those groups
            // assign those groups' shards to the remaining groups
            // The new configuration should divide the shards as evenly as possible
            // among the full set of groups, and should move as few shards as possible 

        case "Move":
            // creating a new configuration in which the shard is assigned to the group

        case "Query":
            // replying with configuration that has the queried number
            // if the number is -1 or bigger than the biggest known configuration number,
            // the shardmaster should reply with the latest configuration.
            // The result of Query(-1) should reflect every Join, Leave or Move RPC that 
            // the shardmaster finished handling before it recevied the Query(-1) RPC

        default:
            panic("Unknown Command!")
        }
    }

    
}

func (sm *KVServer) buildState(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }

    DPrintf("[svserver: %v]\n", sm.me)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
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

    go func(sm *SMServer, persist *raft.Persister) {
        for msg := range sm.applyCh {
            select {
            case <- sm.shutdown:
                return
            default:
                if msg.CommandValid {
                    sm.mu.Lock()
                    sm.handleCmd()
                    sm.mu.Unlock()
                } else {
                    sm.mu.Lock()
                    // InstallSnapshot RPC
                    DPrintf("[kvserver: %v]build state from InstallSnapShot: %v\n", sm.me, msg.Snapshot)
                    sm.buildState(msg.Snapshot)
                    sm.mu.Unlock()
                }
            }
        }
    }

	return sm
}
