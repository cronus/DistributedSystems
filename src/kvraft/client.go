package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    clerkId int64
    commandNum int
    lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    ck.clerkId       = nrand()
    ck.commandNum    = 0
    ck.lastLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// Each of your key/value servers ("kvservers") will have an associated Raft peer. 
// Clerks send Put(), Append(), and Get() RPCs to the kvserver whose associated Raft is the leader. 
// The kvserver code submits the Put/Append/Get operation to Raft, 
// so that the Raft log holds a sequence of Put/Append/Get operations. 
// All of the kvservers execute operations from the Raft log in order, 
// applying the operations to their key/value databases; 
// the intent is for the servers to maintain identical replicas of the key/value database.

// A Clerk sometimes doesn't know which kvserver is the Raft leader. 
// If the Clerk sends an RPC to the wrong kvserver, 
// or if it cannot reach the kvserver, 
// the Clerk should re-try by sending to a different kvserver. 
// If the key/value service commits the operation to its Raft log 
// (and hence applies the operation to the key/value state machine), 
// the leader reports the result to the Clerk by responding to its RPC. 
// If the operation failed to commit (for example, if the leader was replaced), 
// the server reports an error, and the Clerk retries with a different server.

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    hasLeader := false

    getArgs := &GetArgs{
        Key        : key,
        ClerkId    : ck.clerkId,
        CommandNum : ck.commandNum}

    ck.commandNum++

    serverNum := len(ck.servers)

    t0 := time.Now()
    for time.Since(t0).Seconds() < 10 {
        for i := 0; i < serverNum; i++  {
            getReply := new(GetReply)
            ok := ck.servers[(ck.lastLeader + i) % serverNum].Call("KVServer.Get", getArgs, getReply)
            
            if ok && !getReply.WrongLeader {
                hasLeader = true
                ck.lastLeader = (ck.lastLeader + i) % serverNum
                if getReply.Err == OK {
                    return getReply.Value 
                } else if getReply.Err == ErrNoKey {
                    return ""
                }
            }
            DPrintf("[clerk: %v]ok: %v, PutAppend err: %v", ck.clerkId, ok, getReply.Err)
        }
    }
    if !hasLeader {
        DPrintf("[clerk]Fail to reach agreement!\n")
    }
    return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    hasLeader := false
    putappendArgs := &PutAppendArgs{
        Key        : key,
        Value      : value,
        Op         : op,
        ClerkId    : ck.clerkId,
        CommandNum : ck.commandNum}

    ck.commandNum++

    serverNum := len(ck.servers)

    t0 := time.Now()
    for time.Since(t0).Seconds() < 10 {
        for i := 0; i < serverNum; i++  {
            putappendReply := new(PutAppendReply)
            ok := ck.servers[(ck.lastLeader + i) % serverNum].Call("KVServer.PutAppend", putappendArgs, putappendReply)
            
            if ok && !putappendReply.WrongLeader && putappendReply.Err == OK {
                hasLeader = true
                ck.lastLeader = (ck.lastLeader + i) % serverNum
                return
            }
            DPrintf("[clerk: %v]ok: %v, PutAppend err: %v", ck.clerkId, ok, putappendReply.Err)
        }
    }
    if !hasLeader {
        DPrintf("[clerk]PutAppend fail to reach agreement!\n")
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
