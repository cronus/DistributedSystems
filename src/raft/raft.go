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

// import "bytes"
// import "labgob"



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
}

type LogEntry struct {
    LogTerm int
    Command interface{}
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
}

const NULL = -1

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


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
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
    // 1. false if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.VoteGranted  = false
    } else if args.Term > rf.currentTerm {
        rf.votedFor = NULL
    }

    // 2. votedFor is null or candidateId and
    //    candidate's log is at least as up-to-date as receiver's log, then grant vote
    if (rf.votedFor == NULL || rf.votedFor == args.CandidateId) &&
            args.LastLogIndex >= rf.commitIndex &&
            args.LastLogTerm >= rf.logs[rf.commitIndex].LogTerm {
        DPrintf("[RequestVote][server: %v]term :%v voted for:%v. received RequestVote: %v\n", rf.me, rf.currentTerm, rf.votedFor, args)
        reply.Term        = args.Term 
        reply.VoteGranted = true
        rf.currentTerm    = args.Term
        rf.votedFor       = args.CandidateId
        rf.state          = "Follower"
        rf.t.Stop() 
        timeout := time.Duration(300 + rand.Int31n(400))
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
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("[server: %v]Term:%v, server log:%v lastApplied %v, commitIndex: %v, received AppendEntries, %v, arg term: %v, arg log len:%v", rf.me, rf.currentTerm, rf.logs, rf.lastApplied, rf.commitIndex, args, args.Term, len(args.Entries))
    // 1. false if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.Term    = rf.currentTerm
        reply.Success = false
        return
    } 

    rf.currentTerm = args.Term
    rf.state       = "Follower"
    rf.t.Stop() 
    timeout := time.Duration(300 + rand.Int31n(400))
    rf.t.Reset(timeout * time.Millisecond)
    if len(rf.logs) <= args.PrevLogIndex  {
    // 2. false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    } else if rf.logs[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
    // 3. if an existing entry conflicts with a new one (same index but diff terms), 
    //    delete the existing entry and all that follows it 
        rf.logs = rf.logs[:args.PrevLogIndex]
        reply.Term    = rf.currentTerm
        reply.Success = false
        return
    }

    // 4. append any new entries not already in the log
    if len(args.Entries) == 0 {
        DPrintf("[server: %v]received heartbeat\n", rf.me)
    } else if len(rf.logs) == args.PrevLogIndex + 1{
        for i, entry := range args.Entries {
            rf.logs = append(rf.logs[:args.PrevLogIndex + i + 1], entry)
        }
    }

    // 5. if leadercommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if args.LeaderCommit > rf.commitIndex {
        if args.LeaderCommit < len(rf.logs) - 1 {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = len(rf.logs) - 1
        }
        rf.cond.Broadcast()
    }

    reply.Success  = true
    reply.Term     = rf.currentTerm
    return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
        index    = len(rf.logs)
        term     = rf.currentTerm
        isLeader = true

        logEntry        := new(LogEntry)
        logEntry.Command = command
        logEntry.LogTerm = rf.currentTerm
        
        rf.logs = append(rf.logs, *logEntry)

        DPrintf("[server: %v]appendEntriesArgs entry: %v\n", rf.me, *logEntry)

        appendEntriesArgs  := make([]*AppendEntriesArgs, len(rf.peers))
        appendEntriesReply := make([]*AppendEntriesReply, len(rf.peers))

        indexCh := make(chan int)

        for server, _ := range rf.peers {
            if server != rf.me {
                appendEntriesArgs[server] = &AppendEntriesArgs{
                    Term          : rf.currentTerm,
                    LeaderId      : rf.me,
                    PrevLogIndex  : rf.nextIndex[server] - 1,
                    PrevLogTerm   : rf.logs[rf.nextIndex[server] - 1].LogTerm,
                    Entries       : []LogEntry{*logEntry},
                    LeaderCommit  : rf.commitIndex}

                rf.nextIndex[server] += len(appendEntriesArgs[server].Entries)
                DPrintf("leader:%v, nextIndex:%v\n", rf.me, rf.nextIndex)

                appendEntriesReply[server] = new(AppendEntriesReply)

                go func(rf *Raft, server int, args *AppendEntriesArgs, reply *AppendEntriesReply, indexCh chan int) {
                    trialReply := new(AppendEntriesReply)
                    for ok := rf.sendAppendEntries(server, args, trialReply); ; {
                        time.Sleep(50 * time.Millisecond)
                        if rf.state != "Leader" {
                            return
                        }
                        trialReply := new(AppendEntriesReply)
                        ok = rf.sendAppendEntries(server, args, trialReply)
                        if ok && trialReply.Success {
                            reply.Term    = trialReply.Term
                            reply.Success = trialReply.Success
                            rf.mu.Lock()
                            rf.matchIndex[server] = appendEntriesArgs[server].PrevLogIndex + len(appendEntriesArgs[server].Entries)
                            DPrintf("leader:%v, matchIndex:%v\n", rf.me, rf.matchIndex)
                            rf.mu.Unlock()
                            break
                        }
                        if ok && trialReply.Term > args.Term {
                            break
                        }
                    }
                    DPrintf("[server: %v]AppendEntries reply of %v from follower %v, reply:%v\n", rf.me, args, server, reply);
                    indexCh <- server
                    
                }(rf, server, appendEntriesArgs[server], appendEntriesReply[server], indexCh)
            }
        }

        go func (rf *Raft, replies []*AppendEntriesReply, logEntry *LogEntry, indexCh chan int) {

            appendSuccessCntr := 1
            total := 0
            committed := false
            for doneIndex := range indexCh {
                DPrintf("[server: %v]doneIndex: %v logEntry: %v\n", rf.me, doneIndex, logEntry)
                total++
                if replies[doneIndex].Success {
                    appendSuccessCntr++
                }
                if !committed && appendSuccessCntr == len(rf.peers) / 2 + 1 {
                    DPrintf("[server: %v]more than half agreed: %v, on %v\n", rf.me, appendSuccessCntr, logEntry)
                    committed = true
                    rf.mu.Lock()
                    rf.commitIndex++
                    rf.mu.Unlock()
                    rf.cond.Broadcast()
                }
                if total == len(rf.peers) - 1 {
                    DPrintf("close AppendEntries return int channel of logEntry: %v\n", logEntry)
                    close(indexCh)
                }
            }
        }(rf, appendEntriesReply, logEntry, indexCh)


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
    close(rf.shutdown)
}

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

    go func(rf *Raft) {
        timeout := time.Duration(300 + rand.Int31n(400))
        rf.t = time.NewTimer(timeout * time.Millisecond)
        for {
            rf.mu.Lock()
            if rf.state == "Candidate" {
                DPrintf("[server: %v]state:%v\n", rf.me, rf.state)
            }
            select {
            case <-rf.shutdown:
                DPrintf("[server: %v]Close state machine goroutine\n", rf.me);
                return
            default:
                switch rf.state {
                case "Follower":
                    select {
                    case <- rf.t.C:
                        DPrintf("[server: %v]change to candidate\n", rf.me)
                        rf.state = "Candidate"
                    default:
                    }
                    rf.mu.Unlock()

                case "Candidate":
                    requestVoteArgs  := new(RequestVoteArgs)
                    requestVoteReply := make([]*RequestVoteReply, len(peers))

                    // increment currentTerm
                    //rf.currentTerm++
                    // vote for itself
                    rf.votedFor = rf.me
                    grantedCnt := 1
                    // reset election timer
                    rf.t.Reset(timeout * time.Millisecond)
                    // send RequestVote to all other servers
                    requestVoteArgs.Term         = rf.currentTerm + 1
                    requestVoteArgs.CandidateId  = rf.me
                    requestVoteArgs.LastLogIndex = rf.commitIndex
                    requestVoteArgs.LastLogTerm  = rf.logs[rf.commitIndex].LogTerm
                    DPrintf("[server: %v] Candidate, election timeout %v, send RequestVote: %v\n", me, timeout*time.Millisecond, requestVoteArgs);

                    requestVoteReplyChan := make(chan *RequestVoteReply)
                    for server, _ := range peers {
                        if server != me {
                            requestVoteReply[server] = new(RequestVoteReply)
                            go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, replyChan chan *RequestVoteReply) {
                                ok := rf.sendRequestVote(server, args, reply)
                                if ok && reply.VoteGranted {
                                    replyChan <- reply
                                } else {
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
                                rf.t.Stop()
                                timeout := time.Duration(300 + rand.Int31n(400))
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
                                            rf.nextIndex[i]  = len(rf.logs)
                                            rf.matchIndex[i] = 0
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
                            appendEntriesArgs[server] = &AppendEntriesArgs{
                                Term         : rf.currentTerm,
                                LeaderId     : rf.me,
                                PrevLogIndex : rf.nextIndex[server] - 1,
                                PrevLogTerm  : rf.logs[rf.nextIndex[server] - 1].LogTerm,
                                Entries      : nil,
                                LeaderCommit : rf.commitIndex}

                            appendEntriesReply[server] = new(AppendEntriesReply)
                            go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
                                ok := rf.sendAppendEntries(server, args, reply)

                                // if last log index >= nextIndex for a follower:
                                // send AppendEntries RPC with log entries starting at nextIndex
                                // 1) if successful: update nextIndex and matchIndex for follower
                                // 2) if AppendEntries fails because of log inconsistency:
                                //    decrement nextIndex and retry
                                rf.mu.Lock()
                                if ok && !reply.Success && reply.Term == rf.currentTerm {
                                    for {
                                        rf.nextIndex[server]--
                                        detectAppendEntriesArgs := &AppendEntriesArgs{
                                            Term         : rf.currentTerm,
                                            LeaderId     : rf.me,
                                            PrevLogIndex : rf.nextIndex[server] - 1,
                                            PrevLogTerm  : rf.logs[rf.nextIndex[server] - 1].LogTerm,
                                            Entries      : nil,
                                            LeaderCommit : rf.commitIndex}
                                        detectReply := new(AppendEntriesReply)
                                        rf.sendAppendEntries(server, detectAppendEntriesArgs, detectReply)
                                        if detectReply.Success {
                                            break
                                        }
                                    }
                                    DPrintf("[server: %v]Consistency check: nextIndex: %v", rf.me, rf.nextIndex)
                                    forceAppendEntriesArgs := &AppendEntriesArgs{
                                        Term         : rf.currentTerm,
                                        LeaderId     : rf.me,
                                        PrevLogIndex : rf.nextIndex[server] - 1,
                                        PrevLogTerm  : rf.logs[rf.nextIndex[server] - 1].LogTerm,
                                        Entries      : rf.logs[rf.nextIndex[server] : ],
                                        LeaderCommit : rf.commitIndex}

                                    forceReply := new(AppendEntriesReply)
                                    rf.sendAppendEntries(server, forceAppendEntriesArgs, forceReply)
                                    rf.nextIndex[server]  = len(rf.logs)
                                    rf.matchIndex[server] = len(rf.logs) - 1
                                }
                                rf.mu.Unlock()
                            }(server, appendEntriesArgs[server], appendEntriesReply[server])
                        }
                    }

                    // if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N
                    // and log[N].term == currentTerm:
                    // set commitIndex = N

                    rf.mu.Unlock()
                    time.Sleep(period * time.Millisecond)

                }
            }
        }
    }(rf)

    go func(rf *Raft, applyCh chan ApplyMsg) {

        for {
            rf.mu.Lock()
            select {
            case <- rf.shutdown:
                DPrintf("[server: %v]Close logs handling goroutine\n", rf.me)
                rf.mu.Unlock()
                return
            default:
                if rf.lastApplied < rf.commitIndex {
                    DPrintf("[server: %v]lastApplied: %v, commitIndex: %v\n", rf.me, rf.lastApplied, rf.commitIndex);
                    for rf.lastApplied < rf.commitIndex {
                        rf.lastApplied++
                        applyMsg := ApplyMsg {
                            CommandValid: true,
                            Command:      rf.logs[rf.lastApplied].Command,
                            CommandIndex: rf.lastApplied}
                        rf.mu.Unlock()
                        DPrintf("[server: %v]send committed log to service: %v\n", rf.me, applyMsg)
                        applyCh <- applyMsg
                        rf.mu.Lock()
                    }
                } else {
                    rf.cond.Wait()
                }
                rf.mu.Unlock()
            }
        }

    }(rf, applyCh)


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
