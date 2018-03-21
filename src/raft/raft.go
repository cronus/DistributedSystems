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
    Term int
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
    heartBeat chan bool
    shutdown chan struct{}
}

const NULL = -1

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    term = rf.currentTerm
    isleader = rf.state == "Leader"
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
    DPrintf("[server index:term %v:%v]voted for:%v. received RequestVote, candidate:%v, term:%v\n", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term )
    // 1. false if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.VoteGranted  = false
    // TODO problem
    } else if (rf.votedFor == NULL || rf.votedFor == args.CandidateId) &&
            args.LastLogTerm >= rf.logs[rf.lastApplied].Term {
    // 2. votedFor is null or candidateId and
    //    candidate's log is at least as up-to-date as receiver's log, then grant vote
        rf.currentTerm    = args.Term
        rf.votedFor       = args.CandidateId
        reply.Term        = args.Term 
        reply.VoteGranted = true
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
    DPrintf("[server index: %v], received AppendEntries", rf.me)
    // 1. false if term < currentTerm
    if args.Term < rf.currentTerm {
        reply.Success = false
    } else if len(rf.logs) <= args.PrevLogIndex  {
    // 2. false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        reply.Success = false
    } else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
    // 3. if an existing entry conflicts with a new one (same index but diff terms), 
    //    delete the existing entry and all that follows it 
        rf.logs = rf.logs[:args.PrevLogIndex]
    }

    // 4. append any new entries not already in the log
    if len(args.Entries) == 0 {
        DPrintf("received hearbeat\n")
        rf.heartBeat <- true
    } else {
        for _, entry := range args.Entries {
            rf.logs = append(rf.logs, entry)
        }
    }

    // 5. if leadercommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if args.LeaderCommit > rf.commitIndex {
        if args.LeaderCommit < len(rf.logs) {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = len(rf.logs)
        }
    }

    rf.currentTerm = args.Term
    reply.Success  = true
    reply.Term     = rf.currentTerm
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

    // heartbeat chan
    rf.heartBeat = make(chan bool)

    rf.state = "Follower"

    go func(rf *Raft) {
        timeout := time.Duration(400 + rand.Int31n(100))
        t := time.NewTimer(timeout * time.Millisecond)
        var shutdown chan struct{}
        for {
            switch rf.state {
            case "Follower":

                select {
                case <- t.C:
                    rf.state = "Candidate"
                    continue

                // receiving heartbeat
                case <-rf.heartBeat:
                    t.Stop()
                    t.Reset(timeout * time.Millisecond)
                }

            case "Candidate":
                requestVoteArgs  := new(RequestVoteArgs)
                requestVoteReply := make([]*RequestVoteReply, len(peers))

                // vote for itself
                rf.votedFor = rf.me
                grantedCnt := 1
                // reset election timer
                t.Reset(timeout * time.Millisecond)
                // send RequestVote to all other servers
                DPrintf("[server index:%v] Candidate, election timeout, send RequestVote\n", me);
                requestVoteArgs.Term         = rf.currentTerm + 1
                requestVoteArgs.CandidateId  = rf.me
                requestVoteArgs.LastLogIndex = rf.commitIndex
                requestVoteArgs.LastLogTerm  = rf.logs[rf.commitIndex].Term
                ok := make([]bool, len(peers))
                for server, _ := range peers {
                    if server != me {
                        requestVoteReply[server] = new(RequestVoteReply)
                        ok[server] = rf.sendRequestVote(server, requestVoteArgs, requestVoteReply[server])
                    }
                }

                for i := 0; i < len(peers); i++ {
                    if i == rf.me {
                        continue
                    }
                    if ok[i] && requestVoteReply[i].VoteGranted {
                        grantedCnt++ 
                    }
                }
                DPrintf("total granted peers: %v, total peers: %v\n", grantedCnt, len(peers));

                // become leader if receive from majority of servers
                if grantedCnt > len(peers) / 2 + 1 {
                    rf.state = "Leader"
                    // increment currentTerm
                    rf.currentTerm++
                    shutdown = make(chan struct{})

                    // Upon election: send initial hearbeat to each server
                    // repeat during idle period to preven election timeout
                    go func(rf *Raft, shutdown chan struct{}) {
                        period := time.Duration(200)
                        appendEntriesArgs  := new(AppendEntriesArgs)
                        appendEntriesReply := make([]*AppendEntriesReply, len(peers))

                        for {
                            select {
                            case <- shutdown:
                                return
                            default:
                                appendEntriesArgs.Term         = rf.currentTerm
                                appendEntriesArgs.LeaderId     = rf.me
                                appendEntriesArgs.PrevLogIndex = 0
                                appendEntriesArgs.PrevLogTerm  = 0
                                appendEntriesArgs.Entries      = nil
                                appendEntriesArgs.LeaderCommit = 0

                                time.Sleep(period * time.Millisecond)
                                DPrintf("[server index %v]Leader, send hearbeat, period: %v\n", rf.me, period);
                                for server, _ := range peers {
                                    if server != me {
                                        appendEntriesReply[server] = new(AppendEntriesReply)
                                        rf.sendAppendEntries(server, appendEntriesArgs, appendEntriesReply[server])
                                    }
                                }
                            }
                        }
                    }(rf, shutdown)
                    continue
                }

                select {
                // if AppendEntries RPC received from new leader:
                // convert to follower
                case <- rf.heartBeat:
                    rf.state = "Follower"
                    continue
                // election timout elapses: start new election
                case <- t.C:
                    continue
                }

            case "Leader":
                //DPrintf("[server index %v]Leader\n", rf.me);

                // if command received from client:
                // append entry to local log, respond after entry applied to state machine

                // if last log index >= nextIndex for a follower:
                // send AppendEntries RPC with log entries starting at nextIndex
                // 1) if successful: update nextIndex and matchIndex for follower
                // 2) if AppendEntries fails because of log inconsistency:
                //    decremntt nextIndex and retry


                // if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N
                // and log[N].term == currentTerm:
                // set commitIndex = N

            }
        }
        close(shutdown)
    }(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
