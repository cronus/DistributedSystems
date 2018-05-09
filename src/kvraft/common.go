package raftkv

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
    ErrWrongLeader   = "ErrWrongLeader"
    ErrLeaderChanged = "ErrLeaderChanged"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    ClerkId int64
    CommandNum int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    ClerkId int64
    CommandNum int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type KvState struct {
    KvStore     map[string]string
    ReceivedCmd map[int64]int
}
