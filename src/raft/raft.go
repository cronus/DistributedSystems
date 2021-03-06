package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "math/rand"
import "time"

import "bytes"
import "labgob"

import "io"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
    Snapshot []byte
}

type LogEntry struct {
    LogTerm int
    Command interface{}
}

type SnapshotData struct {
    KvState interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers

    // Persistent state on all servers
    // (Updated on stable storage before responding to RPCs)
    // This implementation doesn't use disk; ti will save and restore
    // persistent state from a Persister object
    // Raft should initialize its state from Persister, 
    // and should use it to save its persistent state each tiem the state changes
    // Use ReadRaftState() and SaveRaftState
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    // from Figure 2
    // Persistent state on all servers
    currentTerm int
    votedFor int
    logs []LogEntry
    
    // Volatile state on all servers
    commitIndex int
    lastApplied int

    // Volatile state on leaders:
    // (Reinitialized after election)
    nextIndex []int
    matchIndex []int

    // extra
    state string
    t *time.Timer
    cond *sync.Cond
    shutdown chan struct{}
    applyCh chan ApplyMsg

    // snapshot
    lastIncludedIndex int
    lastIncludedTerm int
    snapshotData []byte
}

const NULL = -1


// keep rf.logs[lastIncludedIndex] at rf.logs[0] after compaction
func (rf *Raft) v2p(vIndex int) (int, bool) {
    
    // check virtual index
    if vIndex - rf.lastIncludedIndex > len(rf.logs) - 1 {
        DPrintf("[server: %v]Warning: v2p(vIndex) greater max log index\n", rf.me)
        return vIndex - rf.lastIncludedIndex, true
    } else if vIndex - rf.lastIncludedIndex >= 0 {
        return vIndex - rf.lastIncludedIndex, true
    } else {
        DPrintf("[server: %v]Warning: v2p(vIndex) less than lastIncludedIndex\n", rf.me)
        return -100, false
    }
}

func (rf *Raft) p2v(pIndex int) int {
    if pIndex > len(rf.logs) - 1 {
        panic("p2v: physical address greater than max log index\n")
    } else {
        return pIndex + rf.lastIncludedIndex
    }
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term     = rf.currentTerm
    isleader = (rf.state == "Leader")
	return term, isleader
}

func (rf *Raft) CompactLog(snapshotData []byte, lastIndex int) {
    
    rf.mu.Lock()
    DPrintf("[server: %v]Compact lastIndex: %v, logs: %v\n", rf.me, lastIndex, rf.logs)
    defer rf.mu.Unlock()

    pLastIndex, _ := rf.v2p(lastIndex)
    lastTerm      := rf.logs[pLastIndex].LogTerm
    rf.persistSnapshotAndState(snapshotData, lastIndex, lastTerm)
    DPrintf("[server: %v] after compact, logs: %v\n", rf.me, rf.logs)
}

func (rf *Raft) persistSnapshotAndState(data []byte, lastIndex int, lastTerm int) {

    DPrintf("[server: %v] persist snapshot and state: lastIndex: %v, rf.lastIncludedIndex: %v, diff: %v\n", rf.me, lastIndex, rf.lastIncludedIndex, lastIndex - rf.lastIncludedIndex)

    pLastIndex, ntInSnp := rf.v2p(lastIndex)
    // discard logs
    if ntInSnp {
        if len(rf.logs) - 1 >= pLastIndex && rf.logs[pLastIndex].LogTerm == lastTerm {
            rf.lastIncludedTerm  = rf.logs[pLastIndex].LogTerm
            rf.logs              = rf.logs[pLastIndex : ]
            rf.logs[0].Command   = nil
            rf.logs[0].LogTerm   = rf.lastIncludedTerm
        } else {
            rf.lastIncludedTerm  = lastTerm
            rf.logs              = rf.logs[:1]
            rf.logs[0].Command   = nil
            rf.logs[0].LogTerm   =  rf.lastIncludedTerm
        }
    } else {
        DPrintf("[server: %v]Already persist snapshot\n", rf.me)
        return
    }

    rf.lastIncludedIndex = lastIndex
    rf.snapshotData      = data

    // encode metadata for snapshot
    buffer   := bytes.NewBuffer(data)
    e        := labgob.NewEncoder(buffer)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    snapshot := buffer.Bytes()

    // encode state
    buffer = new(bytes.Buffer)
    e      = labgob.NewEncoder(buffer)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)

    for i, b := range rf.logs {
        if i == 0 {
            continue
        }
        e.Encode(b.LogTerm)
        e.Encode(&b.Command)
    }
    state := buffer.Bytes()

    // store snapshot in the persister object with 
    // corresponding raft state
    rf.persister.SaveStateAndSnapshot(state, snapshot)

}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { 
		return
	}

    //rf.mu.Lock()
    //defer rf.mu.Unlock()

    //kvStore     := make(map[string]string)
    //receivedCmd := make(map[int]int)
    snapshotData := SnapshotData{}

    buffer := bytes.NewBuffer(data)
    d  := labgob.NewDecoder(buffer)

    // decode kvStore and encode into bytes
    //if err := d.Decode(&kvStore); err != nil {
    //    panic(err)
    //}
    //if err := d.Decode(&receivedCmd); err != nil {
    //    panic(err)
    //}

    if err := d.Decode(&snapshotData.KvState); err != nil {
        panic(err)
    }

    bufferKv := new(bytes.Buffer)
    eKv      := labgob.NewEncoder(bufferKv)
    //eKv.Encode(kvStore)
    //eKv.Encode(receivedCmd)
    eKv.Encode(&snapshotData.KvState)
    rf.snapshotData = bufferKv.Bytes()

    var lastIncludedIndex int
    var lastIncludedTerm int
    // decode lastIncludededIndex and lastIncludedTerm
    if err := d.Decode(&lastIncludedIndex); err != nil {
        panic(err)
    }
    if err := d.Decode(&lastIncludedTerm); err != nil {
        panic(err)
    }
    rf.logs[0].LogTerm   = lastIncludedTerm
    rf.lastIncludedIndex = lastIncludedIndex
    rf.lastIncludedTerm  = lastIncludedTerm
    rf.commitIndex       = rf.lastIncludedIndex
    rf.lastApplied       = rf.lastIncludedIndex
    DPrintf("[server: %v]Read Snapshot: lastIncludedIndex: %v, lastIncludedTerm: %v, log:%v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.logs)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// func (enc *Encoder) Encode(e interface{}) error
//     Encode transmit the data item represented by the empty interface value,
//     guaranteeing that all necessary type information has been transmitted
//     first. Passing a nil pointer to Encoder will panic, as they cannot be
//     transmitted by gob.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
    buffer := new(bytes.Buffer)
    e      := labgob.NewEncoder(buffer)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    for i, b := range rf.logs {
        if i == 0 {
            continue
        }
        //if i > rf.commitIndex {
        //    break
        //}
        //DPrintf("[server: %v]Encode log: %v", rf.me, b)
        e.Encode(b.LogTerm)
        e.Encode(&b.Command)
    }
    state := buffer.Bytes()
    DPrintf("[server: %v]Encode: rf currentTerm: %v, votedFor: %v, log:%v\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
    rf.persister.SaveRaftState(state)
}


//
// restore previously persisted state.
// func (*Deocder) Decode(e interface{}) error
//     Decode reads the next value from the input stream and stores it in 
//     the data represented by the empty interface value. If e is nil, the 
//     value will be discarded.
//     Otherwise, the value underlying e must be a pointer to the correct
//     type for the next data item received. If the input is at EOF, 
//     Decode returns io.EOF and does not modify e
//
func (rf *Raft) readPersist(data []byte) {
    //DPrintf("[server: %v]read persist data: %v, len of data: %v\n", rf.me, data, len(data));
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    buffer := bytes.NewBuffer(data)
    d      := labgob.NewDecoder(buffer)
    var currentTerm int
    var votedFor int
    if d.Decode(&currentTerm) != nil ||
       d.Decode(&votedFor)    != nil {
       DPrintf("error in decode currentTerm and votedFor, err: %v\n", d.Decode(&currentTerm)) 
    } else {
        rf.currentTerm = currentTerm
        rf.votedFor    = votedFor
    }
    for {
        var log LogEntry
        if err := d.Decode(&log.LogTerm); err != nil {
            if err == io.EOF {
                break
            } else {
                DPrintf("error when decode log, err: %v\n", err) 
            }
        }

        if err := d.Decode(&log.Command); err != nil {
            panic(err)
        }
        rf.logs = append(rf.logs, log)
    }
    //rf.commitIndex = len(rf.logs) - 1
    //rf.lastApplied = len(rf.logs) - 1
    DPrintf("[server: %v]Decode: rf currentTerm: %v, votedFor: %v, log:%v, persist data: %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs, data)
    
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("[Enter RequestVote][server: %v]term :%v voted for:%v, log len: %v, logs: %v, commitIndex: %v, received RequestVote: %v\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.logs), rf.logs, rf.commitIndex, args)
    // 1. false if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.Term         = rf.currentTerm
        reply.VoteGranted  = false
    } else if args.Term > rf.currentTerm {
        rf.votedFor = NULL
        reply.Term  = rf.currentTerm
        // need to update follower's term if received candidate RequestVote RPC
        // scenario:
        // [0, 1, 2, 3, 4] all commit to 10, leader 1, term 1
        // [0, 2, 4], no leader, term:1 [1, 3] term: 1
        // [0, 2, 4], leader 0, term:1, use heartbeat to update term [1, 3] term: 1
        // [0] disconnect without sending any heartbeat, [0] term2, [2, 4] term: 1, [1, 3] term:1
        // [0] leader: 0, term: 2, [1, 2, 3, 4] leader:1, term: 1
        // after transaction, [0] still commit to 10, [1, 2, 3, 4] commit to 20
        // [0] rejoin, will become leader, which is not expected
        rf.currentTerm    = args.Term
        rf.state          = "Follower"
    } else {
        reply.Term  = rf.currentTerm
    }

    // 2. votedFor is null or candidateId and
    //    candidate's log is at least as up-to-date as receiver's log, then grant vote
    //    If the logs have last entries with different terms, then the log with the later term is more up-to-date
    //    If the logs end with the same term, then whichever log is longer is more up-to-date

    if (rf.votedFor == NULL || rf.votedFor == args.CandidateId) &&
            ((args.LastLogTerm > rf.logs[len(rf.logs) - 1].LogTerm) || 
                ((args.LastLogTerm == rf.logs[len(rf.logs) - 1].LogTerm) && 
                (args.LastLogIndex >= rf.p2v(len(rf.logs) - 1)))) {
        DPrintf("[RequestVote][server: %v]term :%v voted for:%v, logs: %v, commitIndex: %v, received RequestVote: %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs, rf.commitIndex, args)
        //reply.Term        = rf.currentTerm 
        reply.VoteGranted = true
        rf.votedFor       = args.CandidateId
        if !rf.t.Stop() {
            DPrintf("[server: %v]RequestVote: drain timer\n", rf.me)
            <- rf.t.C
        }
        timeout := time.Duration(500 + rand.Int31n(400))
        rf.t.Reset(timeout * time.Millisecond)
    } else {
        reply.VoteGranted = false
    }
 
    return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries  []LogEntry
    LeaderCommit int
}


type AppendEntriesReply struct {
    Term int
    FirstTermIndex int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("[server: %v]AppendEntries, Term:%v, rf.lastIncludedIndex: %v, server log:%v lastApplied %v, commitIndex: %v, received AppendEntries, %v, arg term: %v, arg log len:%v", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.logs, rf.lastApplied, rf.commitIndex, args, args.Term, len(args.Entries))
    // 1. false if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.Term    = rf.currentTerm
        reply.Success = false
        return
    } 

    rf.state       = "Follower"
    if !rf.t.Stop() {
        DPrintf("[server: %v]AppendEntries: drain timer\n", rf.me)
        <- rf.t.C
    }
    timeout := time.Duration(500 + rand.Int31n(400))
    rf.t.Reset(timeout * time.Millisecond)

    pPrevLogIndex, ntInSnp := rf.v2p(args.PrevLogIndex)

    if ntInSnp {
        if len(rf.logs) - 1 < pPrevLogIndex {
        // 2. false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            DPrintf("[server: %v] log doesn't contain PrevLogIndex\n", rf.me)
            reply.Term           = rf.currentTerm
            reply.FirstTermIndex = rf.p2v(len(rf.logs) - 1) + 1
            reply.Success        = false
            rf.currentTerm       = args.Term
            return
        } else { // args.PrevLogIndex - rf.lastIncludedIndex - 1 < len(rf.logs)
            // 3. if an existing entry conflicts with a new one (same index but diff terms), 
            //    delete the existing entry and all that follows it 
            if rf.logs[pPrevLogIndex].LogTerm != args.PrevLogTerm {
                DPrintf("[server: %v] log contains PrevLogIndex, but term doesn't match\n", rf.me)
                pFirstTermIndex := pPrevLogIndex

                for i := len(rf.logs[ : pFirstTermIndex]) - 1; i >= 0; i-- {
                    pCommitIndex, _ := rf.v2p(rf.commitIndex)
                    if i > pCommitIndex {
                        if rf.logs[i].LogTerm != rf.logs[pPrevLogIndex].LogTerm {
                            pFirstTermIndex      = i + 1
                            reply.FirstTermIndex = rf.p2v(pFirstTermIndex)
                            break
                        }
                    } else {
                        reply.FirstTermIndex = rf.commitIndex + 1
                        pFirstTermIndex, _   = rf.v2p(reply.FirstTermIndex)
                        break
                    }
                }
                DPrintf("[server: %v]FirstTermIndex: %v\n", rf.me, reply.FirstTermIndex)

                //for rf.logs[reply.FirstTermIndex - rf.lastIncludedIndex - 1].LogTerm == rf.logs[reply.FirstTermIndex - 1 - rf.lastIncludedIndex - 1].LogTerm {
                //    if reply.FirstTermIndex > rf.commitIndex {
                //        reply.FirstTermIndex--
                //    } else {
                //        reply.FirstTermIndex = rf.commitIndex + 1
                //        break
                //    }
                //}

                rf.logs        = rf.logs[:pFirstTermIndex]
                reply.Term     = rf.currentTerm
                reply.Success  = false
                rf.currentTerm = args.Term
                return
            }
        }
    } else {
        DPrintf("[server: %v]lastIncludedIndex: %v follower receives AppendEntries from Leader with PrevLogIndex in snapshot, %v\n", rf.me, rf.lastIncludedIndex, args.PrevLogIndex)
        reply.Term = rf.currentTerm
        reply.Success = false
        rf.currentTerm = args.Term
        return
        //panic("unexptected postion, 5\n")
    }

    // 4. append any new entries not already in the log
    if len(args.Entries) == 0 {
        DPrintf("[server: %v]received heartbeat\n", rf.me)
    } else if len(rf.logs) == pPrevLogIndex + 1 {
        for i, entry := range args.Entries {
            rf.logs = append(rf.logs[:pPrevLogIndex + i + 1], entry)
        }
        // persist only when possible committed data
        // for leader, it's easy to determine
        // persist follower whenever update
        rf.persist()
    } else if len(rf.logs) - 1 > pPrevLogIndex && len(rf.logs) - 1 < pPrevLogIndex + len(args.Entries) {
        for i := len(rf.logs); i <= pPrevLogIndex + len(args.Entries); i++ {
            rf.logs = append(rf.logs[:i], args.Entries[i - pPrevLogIndex - 1]) 
        }
        // persist only when possible committed data
        // for leader, it's easy to determine
        // persist follower whenever update
        rf.persist()
    }

    // 5. if leadercommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if args.LeaderCommit > rf.commitIndex {
        if args.LeaderCommit < rf.p2v(len(rf.logs) - 1) {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = rf.p2v(len(rf.logs) - 1)
        }
        rf.cond.Broadcast()
    }

    rf.currentTerm = args.Term
    reply.Success  = true
    reply.Term     = rf.currentTerm
    return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct{
    Term              int
    LeaderId          int
    LastIncludedIndex int
    LastIncludedTerm  int
    Data              []byte
}

type InstallSnapshotReply struct{
    Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("[server: %v]InstallSnapshot. Term: %v, args: %v\n", rf.me, rf.currentTerm, args)

    // 1. Reply immediately if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    }
    
    reply.Term     = rf.currentTerm
    rf.currentTerm = args.Term


    // 4. Reply and wait for more data chunks if done is false (omit)
    // 5. Save snapshot file, discard any existing or partitial snapshot with smaller index
    rf.persistSnapshotAndState(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)

    // 6. If existing log entry has same index and term as snapshot's last 
    //    included entry, retain log entries following it and reply
    //if len(rf.logs) + rf.lastIncludedIndex + 1 > args.LastIncludedIndex && rf.logs[args.LastIncludedIndex - rf.lastIncludedIndex - 1].LogTerm == args.LastIncludedIndex {
    //    rf.logs = rf.logs[args.LastIncludedIndex + 1:]
    //    return
    //}

    // 2.&3. Write data into snapshot file 
    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm  = args.LastIncludedTerm
    rf.snapshotData      = args.Data

    rf.commitIndex       = args.LastIncludedIndex
    rf.lastApplied       = args.LastIncludedIndex

    // 7. Discard the entire log
    //rf.logs = rf.logs[:0]

    // 8. Reset state machine usingsnapshot content (and load snapshot's cluster configuration)
    snapshotApplyMsg := ApplyMsg{
        CommandValid: false,
        Command:      nil,
        CommandIndex: 0,
        Snapshot:     args.Data}

    rf.applyCh <- snapshotApplyMsg

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
    // if command received from client:
    // append entry to local log, respond after entry applied to state machine
    rf.mu.Lock()
    defer rf.mu.Unlock()

    switch rf.state {
    case "Leader":
        index    = rf.p2v(len(rf.logs) - 1) + 1
        term     = rf.currentTerm
        isLeader = true

        logEntry            := new(LogEntry)
        logEntry.LogTerm     = rf.currentTerm
        logEntry.Command     = command
        
        rf.logs = append(rf.logs, *logEntry)

        DPrintf("[server: %v]appendEntriesArgs entry: %v\n", rf.me, *logEntry)

        // persist log when receiving it
        rf.persist()

        appendEntriesArgs  := make([]*AppendEntriesArgs, len(rf.peers))
        appendEntriesReply := make([]*AppendEntriesReply, len(rf.peers))

        for server, _ := range rf.peers {
            if server != rf.me {
                pPrevLogIndex, ntInSnp := rf.v2p(rf.nextIndex[server] - 1)
                if ntInSnp {
                    appendEntriesArgs[server] = &AppendEntriesArgs{
                        Term          : rf.currentTerm,
                        LeaderId      : rf.me,
                        PrevLogIndex  : rf.nextIndex[server] - 1,
                        PrevLogTerm   : rf.logs[pPrevLogIndex].LogTerm,
                        Entries       : []LogEntry{*logEntry},
                        LeaderCommit  : rf.commitIndex}
                } else {
                    panic("1")
                }

                rf.nextIndex[server] += len(appendEntriesArgs[server].Entries)
                DPrintf("leader:%v, nextIndex:%v\n", rf.me, rf.nextIndex)

                appendEntriesReply[server] = new(AppendEntriesReply)

                //go func(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
                //    for {
                //        DPrintf("[server: %v]qwe: %v\n", rf.me, args.Entries[0].Command)
                //        trialReply := new(AppendEntriesReply)
                //        ok := rf.sendAppendEntries(server, args, trialReply)
                //        rf.mu.Lock()
                //        if rf.state != "Leader" {
                //            rf.mu.Unlock()
                //            return
                //        }
                //        if args.Term != rf.currentTerm {
                //            rf.mu.Unlock()
                //            return
                //        }
                //        if ok && trialReply.Success {
                //            reply.Term    = trialReply.Term
                //            reply.Success = trialReply.Success
                //            rf.matchIndex[server] = appendEntriesArgs[server].PrevLogIndex + len(appendEntriesArgs[server].Entries)
                //            DPrintf("leader:%v, matchIndex:%v\n", rf.me, rf.matchIndex)
                //            rf.mu.Unlock()
                //            break
                //        }
                //        if ok && trialReply.Term > rf.currentTerm {
                //            rf.state = "Follower"
                //            rf.currentTerm = trialReply.Term
                //            rf.mu.Unlock()
                //            return
                //        }
                //        rf.mu.Unlock()
                //        time.Sleep(500 * time.Millisecond)
                //    }
                //    DPrintf("[server: %v]AppendEntries reply of %v from follower %v, reply:%v\n", rf.me, args, server, reply);
                //    rf.cond.Broadcast()
                //    
                //}(rf, server, appendEntriesArgs[server], appendEntriesReply[server])
            }
        }

    default:
        isLeader = false
    }

    DPrintf("[server: %v] return value: log index:%v, term:%v, isLeader:%v\n", rf.me, index, term, isLeader)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    rf.mu.Lock()
    defer rf.mu.Unlock()
    close(rf.shutdown)
    rf.cond.Broadcast()
}

//TODO: isolate AppendEntriesArgs as a separate function
//      func (rf *Raft) prepareAppendEntries(hasEntries bool) bool
//      isolate AppendEntries RPC handle as a separate function
//      func (rf *Raft) handleAppendEntriesReply(sendTerm int, reply *AppendEntriesReply) bool
//      preliminary idea: use recersive function is a better solution, and may need
//      auxiliary function 
//      func (rf *Raft) handleInstallSnapshotReply(sendTerm int, reply *InstallSnapshotReply) bool

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

    // currentTerm
    rf.currentTerm = 0
    // votedFor
    rf.votedFor = NULL
    //log
    rf.logs = make([]LogEntry, 1)

    rf.commitIndex = 0
    rf.lastApplied = 0

    rf.state    = "Follower"
    rf.cond     = sync.NewCond(&rf.mu)
    rf.shutdown = make(chan struct{})
    rf.applyCh  = applyCh

    // snapshot metadata
    rf.lastIncludedIndex = 0
    rf.lastIncludedTerm  = 0
    rf.snapshotData = nil

    timeout := time.Duration(300 + rand.Int31n(400))
    rf.t = time.NewTimer(timeout * time.Millisecond)
    go func(rf *Raft) {
        for {
            rf.mu.Lock()
            if rf.state == "Candidate" {
                DPrintf("[server: %v]state:%v\n", rf.me, rf.state)
            }
            select {
            case <-rf.shutdown:
                rf.mu.Unlock()
                DPrintf("[server: %v]Close state machine goroutine\n", rf.me);
                return
            default:
                switch rf.state {
                case "Follower":
                    select {
                    case <- rf.t.C:
                        DPrintf("[server: %v]change to candidate\n", rf.me)
                        rf.state = "Candidate"
                        // reset election timer
                        rf.t.Reset(timeout * time.Millisecond)
                    default:
                    }
                    rf.mu.Unlock()
                    time.Sleep(1 * time.Millisecond)

                case "Candidate":
                    requestVoteArgs  := new(RequestVoteArgs)
                    requestVoteReply := make([]*RequestVoteReply, len(peers))

                    // increment currentTerm
                    //rf.currentTerm++
                    // vote for itself
                    rf.votedFor = rf.me
                    grantedCnt := 1
                    // send RequestVote to all other servers
                    requestVoteArgs.Term         = rf.currentTerm + 1
                    requestVoteArgs.CandidateId  = rf.me
                    requestVoteArgs.LastLogIndex = rf.p2v(len(rf.logs) - 1)
                    requestVoteArgs.LastLogTerm  = rf.logs[len(rf.logs) - 1].LogTerm

                    DPrintf("[server: %v] Candidate, election timeout %v, send RequestVote: %v\n", me, timeout*time.Millisecond, requestVoteArgs);

                    requestVoteReplyChan := make(chan *RequestVoteReply)
                    for server, _ := range peers {
                        if server != me {
                            requestVoteReply[server] = new(RequestVoteReply)
                            go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, replyChan chan *RequestVoteReply) {
                                ok := rf.sendRequestVote(server, args, reply)
                                rf.mu.Lock()
                                if rf.state != "Candidate" {
                                    rf.mu.Unlock()
                                    return
                                }
                                rf.mu.Unlock()
                                if ok && reply.VoteGranted {
                                    replyChan <- reply
                                } else {
                                    rf.mu.Lock()
                                    if reply.Term > rf.currentTerm {
                                        rf.state = "Follower"
                                        rf.currentTerm = reply.Term

                                        // reset timer 
                                        if !rf.t.Stop() {
                                            DPrintf("[server: %v]Leader change to follower1: drain timer\n", rf.me)
                                            <- rf.t.C
                                        }
                                        timeout := time.Duration(500 + rand.Int31n(400))
                                        rf.t.Reset(timeout * time.Millisecond)

                                    }
                                    rf.mu.Unlock()
                                    reply.VoteGranted = false
                                    replyChan <- reply
                                }
                            }(server, requestVoteArgs, requestVoteReply[server], requestVoteReplyChan)
                        }
                    }
                    
                    reply := new(RequestVoteReply)
                    totalReturns := 0
                    loop:
                        for {
                            select {
                            // election timout elapses: start new election
                            case <- rf.t.C:
                                //rf.t.Stop()
                                timeout := time.Duration(500 + rand.Int31n(400))
                                rf.t.Reset(timeout * time.Millisecond)
                                break loop
                            case reply =<- requestVoteReplyChan:
                                totalReturns++
                                if reply.VoteGranted {
                                    grantedCnt++
                                    if grantedCnt > len(peers) / 2 {
                                        rf.currentTerm++
                                        rf.state = "Leader"

                                        rf.nextIndex  = make([]int, len(peers))
                                        rf.matchIndex = make([]int, len(peers))
                                        for i := 0; i < len(peers); i++ {
                                            rf.nextIndex[i]    = rf.p2v(len(rf.logs) - 1) + 1
                                            rf.matchIndex[i]   = rf.p2v(0)
                                        }
                                        break loop
                                    }
                                }
                            default:
                                rf.mu.Unlock()
                                time.Sleep(1 * time.Millisecond)
                                rf.mu.Lock()
                                if rf.state == "Follower" {
                                    break loop
                                }
                            }
                        }

                    DPrintf("[server: %v]Total granted peers: %v, total peers: %v\n", rf.me, grantedCnt, len(peers));
                    rf.mu.Unlock()

                case "Leader":

                    // Upon election: send initial hearbeat to each server
                    // repeat during idle period to preven election timeout
                    period := time.Duration(100)
                    appendEntriesArgs  := make([]*AppendEntriesArgs, len(peers))
                    appendEntriesReply := make([]*AppendEntriesReply, len(peers))

                    DPrintf("[server: %v]Leader, send heartbeat, period: %v\n", rf.me, period*time.Millisecond);
                    for server, _ := range peers {
                        if server != rf.me {

                            pPrevLogIndex, ntInSnp := rf.v2p(rf.nextIndex[server] - 1)
                            if ntInSnp {
                                appendEntriesArgs[server] = &AppendEntriesArgs{
                                    Term         : rf.currentTerm,
                                    LeaderId     : rf.me,
                                    PrevLogIndex : rf.nextIndex[server] - 1,
                                    PrevLogTerm  : rf.logs[pPrevLogIndex].LogTerm,
                                    Entries      : nil,
                                    LeaderCommit : rf.commitIndex}
                            } else {
                                DPrintf("[server: %v]not expected index: %v\n", rf.me, rf.nextIndex[server] - 1)
                                panic(2)
                            }

                            appendEntriesReply[server] = new(AppendEntriesReply)
                            go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
                                ok := rf.sendAppendEntries(server, args, reply)

                                // if last log index >= nextIndex for a follower:
                                // send AppendEntries RPC with log entries starting at nextIndex
                                // 1) if successful: update nextIndex and matchIndex for follower
                                // 2) if AppendEntries fails because of log inconsistency:
                                //    decrement nextIndex and retry
                                rf.mu.Lock()
                                var firstTermIndex int
                                // if get an old RPC reply
                                if args.Term != rf.currentTerm {
                                    rf.mu.Unlock()
                                    return
                                }
                                
                                // check matchIndex in case RPC is lost during reply of log recovery 
                                // log is attached to follower but leader not receive success reply
                                if ok && reply.Success {
                                    if rf.commitIndex < args.PrevLogIndex {
                                        rf.matchIndex[server] = args.PrevLogIndex
                                        rf.cond.Broadcast()
                                    } 
                                } else if ok && !reply.Success {
                                    if rf.state != "Leader" {
                                        rf.mu.Unlock()
                                        return
                                    }
                                    var pPrevLogIndex int
                                    var vPrevLogIndex int
                                    var ntInSnp bool
                                    if reply.Term <= rf.currentTerm {
                                        pPrevLogIndex, ntInSnp = rf.v2p(reply.FirstTermIndex - 1)
                                        if ntInSnp {
                                            vPrevLogIndex = reply.FirstTermIndex - 1
                                        } else {
                                            // InstallSnapshot RPC
                                            installSnapshotArgs  := &InstallSnapshotArgs {
                                                Term              : rf.currentTerm,
                                                LeaderId          : rf.me,
                                                LastIncludedIndex : rf.lastIncludedIndex,
                                                LastIncludedTerm  : rf.lastIncludedTerm,
                                                Data              : rf.snapshotData}
                                            
                                            rf.mu.Unlock()
                                            installSnapshotReply := new(InstallSnapshotReply)
                                            ok3 := rf.sendInstallSnapshot(server, installSnapshotArgs, installSnapshotReply)

                                            rf.mu.Lock()
                                            // handle reply
                                            if !ok3 {
                                                rf.mu.Unlock()
                                                DPrintf("[server: %v]ok3, InstallSnapshot not receive from %v\n", rf.me, server)
                                                return
                                            }
                                            if args.Term != rf.currentTerm {
                                                rf.mu.Unlock()
                                                return
                                            }
                                            if installSnapshotReply.Term > rf.currentTerm {
                                                rf.state = "Follower"
                                                rf.currentTerm = installSnapshotReply.Term

                                                // reset timer 
                                                if !rf.t.Stop() {
                                                    DPrintf("[server: %v]Leader change to follower3: drain timer\n", rf.me)
                                                    <- rf.t.C
                                                }
                                                timeout := time.Duration(500 + rand.Int31n(400))
                                                rf.t.Reset(timeout * time.Millisecond)

                                                rf.mu.Unlock()
                                                return
                                            }
                                            rf.mu.Unlock()
                                            return
                                        }
                                        for {
                                            //rf.nextIndex[server]--
                                            DPrintf("abc:%v, server: %v reply: %v\n", rf, server, reply)

                                            //pPrevLogIndex, pstn = rf.v2p(rf.nextIndex[server] - 1)
                                            detectAppendEntriesArgs := new(AppendEntriesArgs)
                                            if ntInSnp {
                                                detectAppendEntriesArgs = &AppendEntriesArgs{
                                                    Term         : rf.currentTerm,
                                                    LeaderId     : rf.me,
                                                    PrevLogIndex : vPrevLogIndex,
                                                    PrevLogTerm  : rf.logs[pPrevLogIndex].LogTerm,
                                                    Entries      : nil,
                                                    LeaderCommit : rf.commitIndex}
                                            } else {
                                                DPrintf("[server: %v]not expected index\n", rf.me)
                                                panic(3)
                                            }

                                            rf.mu.Unlock()
                                            detectReply := new(AppendEntriesReply)
                                            ok1 := rf.sendAppendEntries(server, detectAppendEntriesArgs, detectReply)
                                            rf.mu.Lock()
                                            if !ok1 {
                                                DPrintf("[server: %v]not receive from %v\n", rf.me, server)
                                                //rf.nextIndex[server] = rf.p2v(len(rf.logs) - 1) + 1
                                                rf.mu.Unlock()
                                                return
                                            }
                                            if ok1 && args.Term != rf.currentTerm {
                                                rf.mu.Unlock()
                                                return
                                            }
                                            if detectReply.Term > rf.currentTerm {
                                                rf.state = "Follower"
                                                rf.currentTerm = detectReply.Term

                                                // reset timer 
                                                if !rf.t.Stop() {
                                                    DPrintf("[server: %v]Leader change to follower1: drain timer\n", rf.me)
                                                    <- rf.t.C
                                                }
                                                timeout := time.Duration(500 + rand.Int31n(400))
                                                rf.t.Reset(timeout * time.Millisecond)

                                                rf.mu.Unlock()
                                                return
                                            }
                                            if detectReply.Success {
                                                firstTermIndex = detectAppendEntriesArgs.PrevLogIndex + 1
                                                break
                                            }
                                            pPrevLogIndex, ntInSnp = rf.v2p(detectReply.FirstTermIndex - 1)
                                            if ntInSnp {
                                                vPrevLogIndex = detectReply.FirstTermIndex - 1
                                            } else {
                                                DPrintf("[server: %v]Leader send InstallSnapshot\n", rf.me)
                                                // send InstallSnapshot RPC
                                                installSnapshotArgs  := &InstallSnapshotArgs {
                                                    Term              : rf.currentTerm,
                                                    LeaderId          : rf.me,
                                                    LastIncludedIndex : rf.lastIncludedIndex,
                                                    LastIncludedTerm  : rf.lastIncludedTerm,
                                                    Data              : rf.snapshotData}
                                                
                                                rf.mu.Unlock()
                                                installSnapshotReply := new(InstallSnapshotReply)
                                                ok4 :=  rf.sendInstallSnapshot(server, installSnapshotArgs, installSnapshotReply)

                                                rf.mu.Lock()
                                                // handle reply
                                                if !ok4 {
                                                    rf.mu.Unlock()
                                                    DPrintf("[server: %v]ok4, InstallSnapshot not receive from %v\n", rf.me, server)
                                                    return
                                                }
                                                if args.Term != rf.currentTerm {
                                                    rf.mu.Unlock()
                                                    return
                                                }
                                                if installSnapshotReply.Term > rf.currentTerm {
                                                    rf.state = "Follower"
                                                    rf.currentTerm = detectReply.Term

                                                    // reset timer 
                                                    if !rf.t.Stop() {
                                                        DPrintf("[server: %v]Leader change to follower4: drain timer\n", rf.me)
                                                        <- rf.t.C
                                                    }
                                                    timeout := time.Duration(500 + rand.Int31n(400))
                                                    rf.t.Reset(timeout * time.Millisecond)

                                                    rf.mu.Unlock()
                                                    return
                                                }
                                                rf.mu.Unlock()
                                                return
                                            }
                                        }
                                        DPrintf("[server: %v]Consistency check: server: %v, firstTermIndex: %v", rf.me, server, firstTermIndex)
                                        
                                        pPrevLogIndex, ntInSnp = rf.v2p(firstTermIndex - 1)
                                        forceAppendEntriesArgs := new(AppendEntriesArgs)
                                        if ntInSnp {
                                            appendEntries := make([]LogEntry, 0)
                                            appendEntries = append(appendEntries, rf.logs[pPrevLogIndex + 1 : ]...)
                                            forceAppendEntriesArgs = &AppendEntriesArgs{
                                                Term         : rf.currentTerm,
                                                LeaderId     : rf.me,
                                                PrevLogIndex : firstTermIndex - 1,
                                                PrevLogTerm  : rf.logs[pPrevLogIndex].LogTerm,
                                                Entries      : appendEntries,
                                                LeaderCommit : rf.commitIndex}
                                        } else {
                                                DPrintf("[server: %v]index is in snapshot: %v\n", rf.me, firstTermIndex)
                                                rf.mu.Unlock()
                                                return
                                                //panic(4)
                                        }

                                        rf.mu.Unlock()
                                        forceReply := new(AppendEntriesReply)
                                        ok2 := rf.sendAppendEntries(server, forceAppendEntriesArgs, forceReply)
                                        rf.mu.Lock()
                                        if ok2 {
                                            if args.Term != rf.currentTerm {
                                                rf.mu.Unlock()
                                                return
                                            }
                                            if forceReply.Term > rf.currentTerm {
                                                rf.state = "Follower"
                                                rf.currentTerm = forceReply.Term

                                                // reset timer 
                                                if !rf.t.Stop() {
                                                    DPrintf("[server: %v]Leader change to follower2: drain timer\n", rf.me)
                                                    <- rf.t.C
                                                }
                                                timeout := time.Duration(500 + rand.Int31n(400))
                                                rf.t.Reset(timeout * time.Millisecond)

                                                rf.mu.Unlock()
                                                return
                                            } else {
                                                if !forceReply.Success {
                                                    rf.mu.Unlock()
                                                    return
                                                }
                                                //rf.nextIndex[server]  = rf.p2v(len(rf.logs) - 1) + 1
                                                rf.matchIndex[server] = forceAppendEntriesArgs.PrevLogIndex + len(forceAppendEntriesArgs.Entries)
                                                DPrintf("[server: %v]successfully append entries: %v, rf.nextIndex: %v\n", rf.me, forceReply, rf.nextIndex)
                                                rf.mu.Unlock()
                                                rf.cond.Broadcast()
                                                return
                                            }
                                        } else {
                                            DPrintf("[server: %v]no reponse from %v\n", rf.me, server)
                                            //rf.nextIndex[server]  = rf.p2v(len(rf.logs) - 1) + 1
                                        }
                                    } else {
                                        rf.state = "Follower"
                                        rf.currentTerm = reply.Term

                                        // reset timer 
                                        if !rf.t.Stop() {
                                            DPrintf("[server: %v]Leader change to follower2: drain timer\n", rf.me)
                                            <- rf.t.C
                                        }
                                        timeout := time.Duration(500 + rand.Int31n(400))
                                        rf.t.Reset(timeout * time.Millisecond)
                                    }
                                }
                                rf.mu.Unlock()
                            }(server, appendEntriesArgs[server], appendEntriesReply[server])
                        }
                    }


                    rf.mu.Unlock()
                    time.Sleep(period * time.Millisecond)

                }
            }
        }
    }(rf)

    go func(rf *Raft, applyCh chan ApplyMsg) {
        for {
            select {
            case <- rf.shutdown:
                DPrintf("[server: %v]Close logs handling goroutine\n", rf.me)
                //rf.mu.Unlock()
                return
            default:
                matchIndexCntr := make(map[int]int)
                rf.mu.Lock()
                // update rf.commitIndex based on matchIndex[]
                // if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N
                // and log[N].term == currentTerm:
                // set commitIndex = N
                if rf.state == "Leader" {
                    rf.matchIndex[rf.me] = rf.p2v(len(rf.logs) - 1)
                    for _, logIndex := range rf.matchIndex {
                        if _, ok := matchIndexCntr[logIndex]; !ok {
                            for _, logIndex2 := range rf.matchIndex {
                                if logIndex <= logIndex2 {
                                    matchIndexCntr[logIndex] += 1 
                                }
                            }
                        }
                    }
                    // find the max matchIndex committed
                    // paper 5.4.2, only log entries from the leader's current term are committed by counting replicas
                    for index, matchNum := range matchIndexCntr {
                        pIndex, _ := rf.v2p(index)
                        if matchNum > len(rf.peers) / 2 && index > rf.commitIndex && rf.logs[pIndex].LogTerm == rf.currentTerm {
                            rf.commitIndex = index
                        }
                    }    
                    DPrintf("[server: %v]matchIndex: %v, cntr: %v, rf.commitIndex: %v\n", rf.me, rf.matchIndex, matchIndexCntr, rf.commitIndex)
                }

                if rf.lastApplied < rf.commitIndex  && rf.commitIndex > rf.lastIncludedIndex {
                    DPrintf("[server: %v]lastApplied: %v, commitIndex: %v\n", rf.me, rf.lastApplied, rf.commitIndex);
                    for rf.lastApplied < rf.commitIndex {
                        rf.lastApplied++
                        pLastApplied, _ := rf.v2p(rf.lastApplied)
                        applyMsg := ApplyMsg {
                            CommandValid: true,
                            Command:      rf.logs[pLastApplied].Command,
                            CommandIndex: rf.lastApplied,
                            Snapshot:     nil}
                        DPrintf("[server: %v]send committed log to service: %v\n", rf.me, applyMsg)
                        rf.mu.Unlock()
                        applyCh <- applyMsg
                        rf.mu.Lock()
                    }
                }
                rf.cond.Wait()
                rf.mu.Unlock()
            }
        }

    }(rf, applyCh)


    rf.readSnapshot(persister.ReadSnapshot())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
