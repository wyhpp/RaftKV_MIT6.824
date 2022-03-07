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
	"6.824-golabs-2020/src/labgob"
	"bytes"
	"log"
	"math/rand"
	"sort"
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
const HEARTBEAT_TIMEOUT = 150
var lock sync.Mutex

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
	SnapShot     PackedSnapShot
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
	term      		 int    //选举轮次
	serverState      ServerState  //服务器状态
	electionTimeout  int   //选举超时时间
	voteFor          int
	heartbeatTime    time.Time

	//2B
	logs			[]LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int    //每个server已知的已复制最新的logentry
	applyCh			chan ApplyMsg

	//3B
	latestSnapshotIndex  int  //最近一次snapshot的最后一个日志位置
}

//logentry结构
type LogEntry struct {
	Term 			int
	Command         interface{}
}

//snapshot结构
type PackedSnapShot struct {
	Database       map[string]string
	Index          int
	LatestSeq      map[int64] int //最近处理的服务器id对应的seqId
	LatestReply    map[int64] string
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
	Term := rf.term
	VoteFor := rf.voteFor
	Logs := rf.logs
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(Term)
		e.Encode(VoteFor)
		e.Encode(Logs)
		data := w.Bytes()
		rf.persister.SaveRaftState(data)

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Term int
	var VoteFor int
	var Logs []LogEntry
	if d.Decode(&Term) != nil ||
	   d.Decode(&VoteFor) != nil ||
		d.Decode(&Logs) != nil{
	  	log.Fatal("raft状态反序列化失败")
	} else {
		rf.mu.Lock()
	  	rf.term = Term
	  	rf.voteFor = VoteFor
	  	rf.logs = Logs
	  	rf.mu.Unlock()
	  	DPrintf("恢复数据 term = %d ,logs = %v",rf.term,rf.logs)
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId   int
	//最新日志的term
	EntryTerm     int
	//最新日志的index
	EntryIndex    int
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

//心跳rpc参数
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Entries		 []LogEntry
	PreLogIndex  int
	PreLogTerm   int
	LeaderCommit int
	LeaderId     int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term          int
	IsSuccess     bool
	ConflictTerm  int
	ConflictFirstIndex  int
}

type SnapshotArgs struct {
	SnapShotBytes  []byte
	Term           int
	logLength      int
	latestSnapshotIndex  int
}

type SnapshotReply struct {
	Term          int
	IsSuccess     bool
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//判断自己当前term是否投过票
	//election restriction : 判断自己的logentry term是否比对方大
	DPrintf("%d 收到投票请求，term is %d ,rf.term = %d",rf.me,args.Term,rf.term)

	reply.VoteGuarantee = true

	rf.mu.Lock()
	if args.Term == rf.term && rf.voteFor != -1 {
		DPrintf("%d 已经投过票给 %d ,term=%d",rf.me,rf.voteFor,args.Term)
		reply.VoteGuarantee = false
	}else if args.Term < rf.term {
		DPrintf("args.term < rf.term")
		reply.VoteGuarantee = false
	}else if len(rf.logs) > 0{
		if args.EntryTerm < rf.logs[len(rf.logs)-1].Term{
			reply.VoteGuarantee = false
		}else if args.EntryTerm == rf.logs[len(rf.logs)-1].Term && args.EntryIndex < len(rf.logs)-1 {
			reply.VoteGuarantee = false
		}
	}

	if reply.VoteGuarantee {
		//包含选举term大于自己的term和选举term等于自己的term但自己没投过票两种
		rf.term = args.Term
		//reply.VoteGuarantee = true
		rf.voteFor = args.CandidateId
		rf.serverState = Follower
		rf.heartbeatTime = time.Now()
		rf.persist()
		DPrintf("%d 投票给 %d , term is %d",rf.me,rf.voteFor,args.Term)
	}

	if rf.term <args.Term {
		rf.term = args.Term
	}

	reply.Term = rf.term
	rf.mu.Unlock()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//重置hearbeattime
	if rf.serverState != Leader {
		rf.serverState = Follower
	}
	//返回自己的term
	//如果收到的term>自己的term
	DPrintf("%d 收到日志 %v",rf.me,args.Entries)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if rf.term <= args.Term{
		rf.term = args.Term
		rf.persist()
		rf.serverState = Follower
		rf.heartbeatTime = time.Now()
		reply.Term = rf.term
	}else {
		DPrintf("%d false 分支1",rf.me)
		reply.IsSuccess = false
		return
	}
	flag := false
	//检测前一个entry的index和term是否符合
	if args.PreLogIndex < len(rf.logs) {
		if args.PreLogIndex == -1 {
			//reply.IsSuccess = true
			flag = true
		}else if rf.logs[args.PreLogIndex].Term == args.PreLogTerm{
			//reply.IsSuccess = true
			//判断是不是已经过期的日志
			lastindex := Min(len(rf.logs[args.PreLogIndex+1:]), len(args.Entries))
			for i := 0; i < lastindex; i++ {
				if rf.logs[args.PreLogIndex+1+i].Term != args.Entries[i].Term {
					flag = true
					break
				}
			}
			//发送的日志和已有日志共同部分相同，但是发送的日志较长
			if !flag && len(args.Entries) > len(rf.logs[args.PreLogIndex+1:]){
				flag = true
			}
		}else {
			//DPrintf("%d false 分支2",rf.me)
			reply.IsSuccess = false
			//日志不一致
			reply.ConflictTerm = rf.logs[args.PreLogIndex].Term
			for i := args.PreLogIndex; i >= 0 ; i-- {
				if rf.logs[i].Term != reply.ConflictTerm {
					reply.ConflictFirstIndex = i+1
					break
				}
			}
			return
		}
	}else{
		DPrintf("%d false 分支3",rf.me)
		reply.IsSuccess = false
		//leader的日志比follower的日志多一个以上
		reply.ConflictFirstIndex = len(rf.logs)-1
		reply.ConflictTerm = -1
		return
	}

	//添加日志
	if flag && len(args.Entries) != 0{
		rf.logs = append(rf.logs[ : args.PreLogIndex+1], args.Entries...)
		rf.persist()
		//if args.LeaderCommit >= rf.commitIndex {
		//	rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
		//}
		DPrintf("%d 日志是 %v,commit index = %d ,len %d",rf.me, rf.logs,rf.commitIndex, len(rf.logs))
	}
	if args.LeaderCommit >= rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
	}
	reply.IsSuccess = true
	//每个服务器判断是否需要提交日志
	if rf.lastApplied < rf.commitIndex{
		DPrintf("%d,commit index = %d",rf.me,rf.commitIndex)
		go rf.commitLogs()
	}
	return

}

//开启选举流程
func (rf *Raft) startElection(cond *sync.Cond) {
	for  {

		//1.将自己变成candidate
		rf.mu.Lock()
		cond.Wait()
		DPrintf("server %d start election,term is %d",rf.me,rf.term+1)
		rf.serverState = Candidate
		rf.electionTimeout = HEARTBEAT_TIMEOUT + 50 + rand.Int()%150 //150-300ms
		rf.term = rf.term + 1
		rf.voteFor = rf.me
		rf.heartbeatTime =time.Now()
		rf.mu.Unlock()
		rf.persist()
		//rf.voteFor = -1
		//2.投票给自己，开始计时
		count := 1
		finish := 0

		//3.并发给其他服务器发送投票请求
		//var wg sync.WaitGroup
		cn := make(chan int)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(x int) {
				args := RequestVoteArgs{
					CandidateId: rf.me,
					Term: rf.term,
					EntryIndex: len(rf.logs)-1,
				}
				if len(rf.logs) == 0 {
					args.EntryTerm = 0
				}else {
					args.EntryTerm = rf.logs[len(rf.logs)-1].Term
				}
				reply := RequestVoteReply{}
				DPrintf("%d send vote  request to %d ,term is %d",rf.me,x,rf.term)
				ok := rf.sendRequestVote(x,&args,&reply)
				DPrintf("%d get reply vote is %v , term is %d",rf.me,reply.VoteGuarantee,rf.term)
				rf.mu.Lock()
				if ok {
					if reply.VoteGuarantee && reply.Term == rf.term{
						count++
					}
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.serverState = Follower
						rf.persist()
					}
				}
				finish++
				rf.mu.Unlock()
				cn<- 1
			}(i)
		}
		//4.接收投票并统计
		//如果选举超时，则再次发起选举
		//go rf.testElectionTimeout()
		//go rf.processVoteReply(cn,cond,count,finish)
		for rf.serverState == Candidate {
			//如果收到半数以上的票数，当选为leader
			<-cn
			//DPrintf("channel 数据 %d ",data)
			rf.mu.Lock()
			if count > len(rf.peers)/2 {
				DPrintf("leader is %d , term is %d",rf.me,rf.term)
				rf.serverState = Leader
				//初始化nextindex
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.logs)
				}
				//如果当选leader，周期发送心跳验证
				cond.Broadcast()
				rf.mu.Unlock()
				break
			}

			if finish == len(rf.peers)-1 {
				cond.Broadcast()
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
		DPrintf("选举结束，term %d",rf.term)

	}

}

//func (rf *Raft)processVoteReply(cn chan int,cond *sync.Cond,count int,finish int)  {
//	//4.接收投票并统计
//	//如果选举超时，则再次发起选举
//	//go rf.testElectionTimeout()
//
//	for rf.serverState == Candidate {
//		//如果收到半数以上的票数，当选为leader
//		<-cn
//		//DPrintf("channel 数据 %d ",data)
//		rf.mu.Lock()
//		if count > len(rf.peers)/2 {
//			DPrintf("leader is %d , term is %d",rf.me,rf.term)
//			rf.serverState = Leader
//			//初始化nextindex
//			for i := range rf.nextIndex {
//				rf.nextIndex[i] = len(rf.logs)
//			}
//			//如果当选leader，周期发送心跳验证
//			cond.Broadcast()
//			rf.mu.Unlock()
//			break
//		}
//
//		if finish == len(rf.peers)-1 {
//			//cond.Broadcast()
//			rf.mu.Unlock()
//			break
//		}
//		rf.mu.Unlock()
//	}
//	DPrintf("选举结束，term %d",rf.term)
//}
//周期发送心跳验证
func (rf *Raft) sendHB(cond *sync.Cond){
	for {
		rf.mu.Lock()
		for rf.serverState != Leader {
			cond.Wait()
		}
		if rf.serverState == Leader {
			DPrintf("%d 发送心跳验证",rf.me)
			//同时发送心跳验证
			go rf.logReplicate()
			go rf.SnapshotReplicate()
		}
		rf.mu.Unlock()
		time.Sleep((HEARTBEAT_TIMEOUT) * time.Millisecond)
	}
	DPrintf("%d 不是leader ,退出心跳发送", rf.me)
}

func (rf *Raft) logReplicate() {
	count := 1
	finish := 0
	cn := make(chan int)

	//发送日志给所有follower
	if rf.serverState == Leader {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.nextIndex[i] <= rf.latestSnapshotIndex {
				continue
			}
			go func(x int) {
				args := AppendEntriesArgs{
					Term:        rf.term,
					PreLogIndex: rf.nextIndex[x] - 1, //对应follower日志的nextIndex的前一个index
					Entries:     rf.logs[rf.nextIndex[x]:],
					LeaderCommit: rf.commitIndex,
					LeaderId:     rf.me,
				}
				//初始没有日志的状态
				if rf.nextIndex[x] == 0 {
					args.PreLogTerm = -1
				} else {
					args.PreLogTerm = rf.logs[rf.nextIndex[x]-1].Term
					//args.Entries = rf.logs[rf.nextIndex[x]:] //对应服务器下一个index位置的日志到日志末尾
				}
				//发送了空日志
				fileEnd := len(rf.logs)
				flag := false
				if rf.nextIndex[x] == len(rf.logs) {
					flag = true
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(x, &args, &reply)
				rf.mu.Lock()
				if ok {
					//DPrintf("%d 返回true",x)
					DPrintf("%d conflictindex = %d",x,reply.ConflictFirstIndex)
					//比较返回的term，如果大于leader的term,则leader应该退出
					if reply.Term > rf.term {
						DPrintf("%d leader 日志不是最新，退出leader",rf.me)
						rf.term = reply.Term
						rf.serverState = Follower
						rf.persist()
						//rf.heartbeatTime = time.Now()
					}
					//如果term小于rf.term，则丢弃
					if reply.IsSuccess && reply.Term == rf.term{
						count++
						//更新matchIndex
						//如果发送的是空日志，不能吧nextIndex加一,也不能更新matchIndex
						if !flag {
							rf.nextIndex[x] = fileEnd
							rf.matchIndex[x] = rf.nextIndex[x]-1
						}
					} else if !reply.IsSuccess && reply.Term >= rf.term {
						//follower同步失败，回退nextindex
						//比较返回的term，如果大于leader的term,则leader应该退出
						DPrintf("follower %d 同步日志失败",x)
						if len(rf.logs)>0 {
							//检查日志在confictfirstindex位置的term是否和conflicterm一致
							//如果ConflictFirstIndex = -1,说明follower目前没有日志
							//DPrintf("rf = %d , reply = %d",rf.logs[reply.ConflictFirstIndex].Term,reply.ConflictTerm)
							if reply.ConflictFirstIndex == -1 {
								rf.nextIndex[x] = 0
							}else if rf.logs[reply.ConflictFirstIndex].Term == reply.ConflictTerm {
								//向后遍历直到不一致为止
								for i := reply.ConflictFirstIndex+1; i < len(rf.logs); i++ {
									if rf.logs[i].Term != reply.ConflictTerm {
										rf.nextIndex[x] = i
										break
									}
								}
								//rf.nextIndex[x] = reply.ConflictFirstIndex+1
								DPrintf("%d xxx",x)
							}else {
								rf.nextIndex[x] = reply.ConflictFirstIndex
								DPrintf("%d yyy",x)
							}
						}
					}
				}
				finish++
				rf.mu.Unlock()
				cn <- 1
			}(i)
		}

		for rf.serverState == Leader {
			<-cn
			rf.mu.Lock()
			if count > len(rf.peers)/2 {
				DPrintf("leader %d 日志同步成功", rf.me)
				//超过半数服务器返回成功，认为成功，commit日志
				//只commit 最新的logentry
				if rf.serverState ==Leader {
					//1.提交的日志term必须等于当前term
					//2.提交日志的index应当小于等于(存在n,使得大多数matchIndex[x]>=n)
					rf.matchIndex[rf.me] = len(rf.logs)-1
					sortMatchIndex := rf.matchIndex
					//升序排序
					sort.Ints(sortMatchIndex)
					//取数组中间
					N := sortMatchIndex[len(rf.peers)/2]
					if N > rf.commitIndex && len(rf.logs)>N && rf.logs[N].Term == rf.term {
						rf.commitIndex = N
					}
					//commitlog
					if rf.lastApplied < rf.commitIndex {
						//leader准备commit,通知server
						go rf.commitLogs()
					}
				}
				rf.mu.Unlock()
				break
			}
			//所有的server均已回应，无论是否掉线
			if finish == len(rf.peers)-1 {
				DPrintf("leader %d 所有服务器已回复", rf.me)
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
	}

}

//选举超时检测
func (rf *Raft)testElectionTimeout(cond *sync.Cond){
	for true {
		//如果选举超时，则再次发起选举
		if rf.killed(){
			DPrintf("%d shutdown退出计时",rf.me)
			break
		}
		if rf.serverState != Leader && time.Now().Sub(rf.heartbeatTime) > time.Duration(rf.electionTimeout)*time.Millisecond {
			DPrintf("%d 选举超时 term is %d",rf.me,rf.term)
			cond.Broadcast()
		}
		time.Sleep(100*time.Millisecond)
	}
}
//commit日志
func (rf *Raft)commitLogs()  {
	rf.mu.Lock()

	//出现一种情况：commitIndex和len(logs)相等，但是切片rf.logs[rf.lastApplied+1:rf.commitIndex+1]有值，且长度大于rf.logs,
	//原因是append函数将特定位置的值替换为新值，并且修改logs的边界范围，但是原本存在右边界之外的值没有改，因此能取到值，但不是安全的
	DPrintf("=======%d 提交日志 %v ,日志 %v",rf.me,rf.logs[rf.lastApplied+1:rf.commitIndex+1],rf.logs)
	if rf.commitIndex > len(rf.logs)-1 {
		//DPrintf("commitindex = %d ,%d 日志 %v",rf.commitIndex, rf.me,rf.logs)
		log.Fatal("日志提交出现错误：rf.commitLogs()")
	}
	for i := rf.lastApplied+1; i <=rf.commitIndex ; i++ {
		rf.applyCh<-ApplyMsg{
			CommandValid: true,
			CommandIndex: i+1,
			Command: rf.logs[i].Command,
			}
	}
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

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
	//模拟客户端给服务器发送命令
	//leader接到命令后，进行操作，同步日志，发送给follower
	//follower接到命令返回false
	index := -1
	term := rf.term
	var isLeader bool
	if rf.serverState == Leader {
		isLeader = true
	}else {
		isLeader = false
		return index, term, isLeader
	}

	// Your code here (2B).
	//将命令加到日志末尾
	rf.mu.Lock()
	index = len(rf.logs)+1
	term = rf.term
	entry := LogEntry{
		Term: rf.term,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	rf.persist()
	rf.mu.Unlock()
	DPrintf("%d leader的日志 %v",rf.me, rf.logs)
	//如果是leader
	//go rf.startAgreement(command)

	return index, term, isLeader
}

//func (rf *Raft)startAgreement(command interface{}){
//	//将命令加到日志末尾
//	rf.mu.Lock()
//	entry := LogEntry{
//		Term: rf.term,
//		Command: command,
//	}
//	rf.logs = append(rf.logs, entry)
//	rf.mu.Unlock()
//	DPrintf("%d leader的日志 %v",rf.me, rf.logs)
//	//rf.logReplicate()
//	//这边不管是不是超过半数了，反正周期心跳也会同步日志
//}
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
	rf.electionTimeout = HEARTBEAT_TIMEOUT
	//2B
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}
	rf.logs = make([]LogEntry, 0)
	rf.applyCh = applyCh
	rf.lastApplied = -1
	rf.commitIndex = -1

	rf.latestSnapshotIndex = -1
	//rf.isDown = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//初始化完成开始选举流程
	//开一个线程发送投票，同时开一个线程接收投票
	cond := sync.NewCond(&rf.mu)
	go rf.startElection(cond)

	//检测heartbeat是否超时，如果超时则重新发起选举
	//go rf.testHB()
	go rf.testElectionTimeout(cond)

	go rf.sendHB(cond)

	return rf
}

func (rf *Raft)GetLogSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.persister.raftstate)
}
//leader收到日志进行截断和持久化
func (rf *Raft)Snapshot(db map[string]string,index int,latestSeq map[int64]int,latestReply map[int64]string)  {
	//通知
	if rf.serverState != Leader {
		return
	}
	snapShot := PackedSnapShot{
		Database:    db,
		Index:       index,
		LatestSeq:   latestSeq,
		LatestReply: latestReply,
	}
	rf.latestSnapshotIndex = index
	rf.logs = rf.logs[index-1 :]
	//序列化
	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(snapShot)
	data1 := w1.Bytes()
	rf.saveSnapshot(data1)

}

func (rf *Raft) SnapshotReplicate() {
	//发送给follower并等待返回
	snapShot := rf.persister.ReadSnapshot()
	//发送日志给所有follower
	if rf.serverState == Leader {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.nextIndex[i] > rf.latestSnapshotIndex {
				continue
			}
			go func(x int) {
				args := SnapshotArgs{
					Term:           rf.term,
					logLength:      rf.latestSnapshotIndex,
					SnapShotBytes: snapShot,
					latestSnapshotIndex: rf.latestSnapshotIndex,
				}
				reply := SnapshotReply{}
				ok := rf.sendSnapshot(x, &args, &reply)
				rf.mu.Lock()
				if ok {
					if reply.Term > rf.term {
						rf.term = args.Term
						rf.serverState = Follower
						rf.voteFor = -1
						rf.persist()
						rf.mu.Unlock()
						return
					}
					//nextindex移到leader日志末尾
					rf.nextIndex[x] = len(rf.logs)
					rf.matchIndex[x] -= args.logLength
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}


func (rf *Raft) sendSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	return ok
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = args.Term
	if args.Term < rf.term {
		reply.IsSuccess = false
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.serverState = Follower
		rf.voteFor = -1
		rf.persist()
	}
	rf.heartbeatTime = time.Now()
	//如果leader的快照不如自己的新，则放弃该快照
	if args.latestSnapshotIndex <= rf.latestSnapshotIndex {
		reply.IsSuccess = false
		return
	}else {
		//快照日志截取的位置比现有日志短
		if args.latestSnapshotIndex <= len(rf.logs) {
			leftLogs := make([]LogEntry, len(rf.logs)-args.logLength)
			copy(leftLogs,rf.logs[args.latestSnapshotIndex :])
			rf.logs = leftLogs
			rf.commitIndex = rf.commitIndex-args.logLength
			rf.lastApplied = rf.commitIndex-args.logLength
		}else {
			rf.logs = make([]LogEntry,0)
			rf.commitIndex = -1
			rf.lastApplied = -1
		}
		rf.latestSnapshotIndex = args.latestSnapshotIndex
	}

	rf.saveSnapshot(args.SnapShotBytes)
	go rf.installSnapshotToServer(args.SnapShotBytes)
	reply.IsSuccess = true
}

func (rf *Raft)saveSnapshot(snapshot []byte)  {
	Term := rf.term
	VoteFor := rf.voteFor
	Logs := rf.logs
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Term)
	e.Encode(VoteFor)
	e.Encode(Logs)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data,snapshot)
}
//把snapshot发送到server层执行
func (rf *Raft)installSnapshotToServer(shot []byte)  {
	r := bytes.NewBuffer(shot)
	d := labgob.NewDecoder(r)
	snapShot := PackedSnapShot{}
	var Database       map[string]string
	var Index          int
	var LatestSeq      map[int64] int //最近处理的服务器id对应的seqId
	var LatestReply    map[int64] string
	if d.Decode(&Database) != nil ||
		d.Decode(&Index) != nil ||
		d.Decode(&LatestSeq) != nil ||
		d.Decode(&LatestReply) != nil{
		log.Fatal("raft状态反序列化失败")
	} else {
		snapShot.LatestSeq = LatestSeq
		snapShot.Index = Index
		snapShot.LatestReply = LatestReply
		snapShot.Database = Database
		//DPrintf("恢复数据 term = %d ,logs = %v",rf.term,rf.logs)
	}
	rf.mu.Lock()
	rf.applyCh<-ApplyMsg{
		CommandValid: false,
		SnapShot: snapShot,
	}
	rf.mu.Unlock()
}