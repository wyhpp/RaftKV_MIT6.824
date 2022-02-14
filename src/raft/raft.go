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

//// create a new Raft server instance:
//rf := Make(peers, me, persister, applyCh)
//
//// start agreement on a new log entry:
//rf.Start(command interface{}) (index, term, isleader)
//
//// ask a Raft for its current term, and whether it thinks it is leader
//rf.GetState() (term, isLeader)
//
//// each time a new entry is committed to the log, each Raft peer
//// should send an ApplyMsg to the service (or tester).
//type ApplyMsg

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824-golabs-2020/src/labrpc"

// import "bytes"
// import "../labgob"

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

//心跳超时时间
const HEARTBEAT_TIMEOUT = 100

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2A
	term      int    //选举轮次
	serverState   ServerState  //服务器状态
	electionTimeout  int   //选举超时时间
	voteFor          int
	heartbeatTime     time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	if rf.serverState == Leader {
		isleader = true
	}else {
		isleader = false
	}
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
	CandidateId   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term          int
	VoteGuarantee bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//判断自己当前term是否投过票
	if args.Term == rf.term && rf.voteFor != -1 {
		reply.VoteGuarantee = false
	}
	if args.Term < rf.term {
		reply.VoteGuarantee = false
	}else {
		//包含选举term大于自己的term和选举term等于自己的term但自己没投过票两种
		rf.term = args.Term
		reply.VoteGuarantee = true
		rf.voteFor = args.CandidateId
		DPrintf("%d 投票给 %d , term is %d",rf.me,rf.voteFor,rf.term)
	}
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

	if ok {
		return reply.VoteGuarantee
	}else {
		reply.VoteGuarantee = false
	}
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.replyHeartBeat", args, reply)
	if ok {
		if reply.Term > rf.term {
			//重新发起选举
		}
	}
	return ok
}

func (rf *Raft) replyHeartBeat(args *RequestVoteArgs, reply *RequestVoteReply) {
	//重置hearbeattime
	rf.heartbeatTime = time.Now()
	//返回自己的term
	//如果收到的term>自己的term
	rf.serverState = Follower
	if args.Term > rf.term {
		rf.term = args.Term
	}else if args.Term < rf.term {

	}

	reply.Term = rf.term
}

//开启选举流程
func (rf *Raft) startElection() bool{
	//1.将自己变成candidate
	rf.serverState = Candidate
	rf.electionTimeout = 150 + rand.Intn(150) //150-300ms
	rf.term = rf.term + 1
	//rf.voteFor = -1
	//2.投票给自己，开始计时
	count := 1
	rf.voteFor = rf.me
	startTime :=time.Now()
	//3.并发给其他服务器发送投票请求
	//var wg sync.WaitGroup
	var mu sync.Mutex
	var cn chan int
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(x int) {
			args := RequestVoteArgs{
				CandidateId: x,
				Term: rf.term,
			}
			reply := RequestVoteReply{}
			getReply := rf.sendRequestVote(x,&args,&reply)
			if getReply {
				mu.Lock()
				defer mu.Unlock()
				if reply.VoteGuarantee {
					count++
				}
			}
			cn<- 1
		}(i)
	}
	//4.接收投票并统计
	for true {
		//如果收到半数以上的票数，当选为leader
		<-cn
		mu.Lock()
		defer mu.Unlock()
		if count > len(rf.peers)/2 {
			DPrintf("leader is %d , term is %d",rf.me,rf.term)
			rf.serverState = Leader
			break
		}
		//如果选举超时，则再次发起选举
		if time.Now().Sub(startTime) > time.Duration(rf.electionTimeout)*time.Millisecond {
			DPrintf("选举超时 term is %d",rf.term)
			return false
		}
		//如果收到leader的心跳验证，自己落选
		if rf.serverState == Follower {
			break
		}
	}
	DPrintf("选举结束，term %d",rf.term)
	return true
}

//周期发送心跳验证
func (rf *Raft) sendHB(){
	for rf.serverState == Leader {
		time.Sleep(100*time.Millisecond)
		//同时发送心跳验证
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(x int) {
				args := RequestVoteArgs{
					CandidateId: x,
					Term:        rf.term,
				}
				reply := RequestVoteReply{}
				rf.sendHeartBeat(x, &args, &reply)
			}(i)
		}
	}
}

//检测heartbeat是否超时
func (rf *Raft) testHB(){

	for {
		if rf.serverState != Leader {
			if time.Now().Sub(rf.heartbeatTime) > HEARTBEAT_TIMEOUT*time.Millisecond {
				rf.startElection()
			}
		}
		time.Sleep(HEARTBEAT_TIMEOUT*time.Millisecond)
	}
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


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.term = 0
	rf.serverState = Follower
	rf.voteFor = -1
	rf.heartbeatTime = time.Now()
	//rf.electionTimeout = 150+rand.Intn(150)   //150-300ms
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//初始化完成开始选举流程
	//开一个线程发送投票，同时开一个线程接收投票
	for  {
		DPrintf("server %d start election ",rf.me)
		if rf.startElection() {
			break
		}
	}

	//如果当选leader，周期发送心跳验证
	go rf.sendHB()

	//检测heartbeat是否超时，如果超时则重新发起选举
	go rf.testHB()

	return rf
}
