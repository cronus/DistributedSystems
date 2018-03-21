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
    term int
    command interface{}
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
    term int
    candidateId int
    latLogIndex int
    lastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    term int
    voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

    // 1. false if term < currentTerm
    if args.term < rf.currentTerm {
        reply.voteGranted  = false
    } 
    // 2. votedFor is null or candidateId and
    //    candidate's log is at least as up-to-date as receiver's log grant vote
    else if rf.votedFor == nil || args.candidateId == rf.votedFor &&
            args.lastLogTerm >= rf.lastApplied {
        rf.currentTerm    = args.term
        reply.term        = args.term ??
        reply.voteGranted = true
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
    term int
    leaderId int
    prevLogIndex int
    preLogTerm int
    entries  []LogEntry
    leaderCommit int
}


type AppendEntriesReply struct {
    term int
    success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

    // 1. false if term < currentTerm
    if args.term < rf.currentTerm {
        reply.success = false
    }
    // 2. false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    else if len(rf.logs) <= args.prevLogIndex || rf.logs[args.prevLogIndex].term != args.prevLogTerm {
        reply.success = false
    }

    // 3. if an existing entry conflicts with a new one (same index but diff terms), 
    //    delete the existing entry and all that follows it 

    // 4. append any new entries not already in the log
    for _, entry = range entries {
        rf.logs = append(rf.logs, entry)
    }

    // 5. if leadercommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if leaderCommit > rf.commitIndex {
        if leaderCommit < index of last new entry {
            rf.commitIndex = leaderCommit
        }
        else {
            rf.commitIndex = index of last new entry
        }
    }
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
    rf.votedFor = nil
    //log
    rf.logs = make([]LogEntry)
    // heartbeat chan
    rf.heartBeat = make(chan bool)

    go func(rf *Raft) {
        timeout := 300 + rand.Int31n(100)
        for {
            switch rf.state {
            case "Follower":
                requestVoteArgs  = new(RequestVoteArgs)
                requestVoteReply = new(RequestVoteReply, len(peers))
                t := NewTimer(timeout * time.Millisecond)

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
                t.Reset(timeout * time.Millisecond)

                requestVoteArgs.term         = rf.currentTerm
                requestVoteArgs.candidateId  = rf.me
                requestVoteArgs.lastLogIndex = rf.commitIndex
                requestVoteArgs.lastLogTerm  = rf.logs[commitIndex].term

                fmt.Printf("leader: %v, election timeout, send Request Vote", me);
                var ok []bool
                for server, peer := range peers {
                    ok[server] := rf.sendRequestVote(server, requestVoteArgs, requestVoteReply[server])
                }

                select {
                case <- t.C:
                    continue

                // receiving heartbeat
                case <- heartBeat:
                    rf.state = "Follower"
                    continue

                }

            case "Leader":
                // heartbeat
                go func(rf *Raft) {
                    period := 200
                    appendEntriesArgs  := new(AppendEntriesArgs)
                    appendEntriesReply := new(AppendEntriesReply, len(peers))

                    for {
                        appendEntriesArgs.term         = rf.currentTerm
                        appendEntriesArgs.leadId       = rf.me
                        appendEntriesArgs.prefLogIndex = nil
                        appendEntriesArgs.preLogTerm   = nil
                        appendEntriesArgs.entries      = nil
                        appendEntriesArgs.leaderCommit = nil

                        time.Sleep(period * time.Millisecond)
                        fmt.printf("leader: %v, send hearbeat, period: %v\n", rf.me, period);
                        for server, peer := range peers {
                            if server != me {
                                rf.sendAppendEntries(server, appendEntriesArgs, appendEntriesReply[server])
                            }
                        }
                    }
                }(rf)
            }
        }
    }(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
