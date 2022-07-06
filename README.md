# MIT6.824 Lab1-3总结



## 1 Lab1 MapReduce

功能：实现一个分布式单词计数器



## 2 Lab2-3 Raft

- 功能：根据Raft算法实现一个分布式KV数据库
- Lab2A：实现leader选举和心跳发送
- Lab2B：实现通过RPC发送和接受日志
- Lab2C：实现节点状态持久化
- Lab3A：实现KV Client和Service层
- Lab3B：实现日志压缩和数据库快照



参考网址：

[Raft 协议实战系列（五）—— 集群成员变更与日志压缩 - 掘金 (juejin.cn)](https://juejin.cn/post/6902274909959880711)

## 3 Raft

​	Raft是基于状态机的分布式算法。Raft将一致性分解成了多个子问题：Leader Election、Log Replication、Safety、Log Compaction、Membership Change等



### 角色分类

Raft中的角色分为Leader、Follower和Candidate

- **Leader**：接受客户端请求，并向Follower同步请求日志，当日志同步到大多数节点上后告诉Follower提交日志。
- **Follower**：接受并持久化Leader同步的日志，在Leader告之日志可以提交之后，提交日志。
- **Candidate**：Leader选举过程中的临时角色。



Raft 有几个基本性质：

1. **Election Safety**：一个 Term 内最多只能有一个 Leader。
2. **Leader Append-Only**：Leader 只会往自己的 Log 中新增日志，不会删除、修改日志。
3. **Log Matching**：如果某两个 Log 有相同的 Index 和 Term，那么这个 Log 以及之前的所有 Log 都包含相同的数据。
4. **Leader Completeness**：如果一个日志已经提交（Commited），那么这个日志将会包含在所有未来的有着更高 Term 的 Leader 中。
5. **State Machine Safety**：如果一个日志已经应用（Applied）到状态机，那么不会有服务器在同一个 Index 应用不同的 Log。

同时，还有几个实现时候的原则：

1. Log 只会由 Leader 发送到 Follower，不可能反向传输。
2. 状态机只能在一个操作被 Apply 之后，才能将这个操作应用到状态转移。
3. Log 应该按顺序 Apply，不应该出现空洞，例如，应用 Index 为 1 的 Log 之后如果应用 Index 为 3 的 Log 是非法的。

#### 结构体定义

1. raft state

   ```Go
   //raft server state
   type Raft struct {
   	mu        sync.Mutex          // Lock to protect shared access to this peer's state
   	peers     []*labrpc.ClientEnd // RPC end points of all peers
   	persister *Persister          // Object to hold this peer's persisted state
   	me        int                 // this peer's index into peers[]
   	dead      int32               // set by Kill()
   
   	//2A
   	term      		 int    //选举轮次
   	serverState      ServerState  //服务器状态
   	electionTimeout  int   //选举超时时间
   	voteFor          int   //本轮次投票给的节点编号，默认-1
   	heartbeatTime    time.Time  //初始化心跳时间
   
   	//2B
   	logs			[]LogEntry   //server日志
   	commitIndex     int			 //需要提交日志index
   	lastApplied     int			 //上次已提交日志index
   	nextIndex       []int		 //	leader存放，leader下次发送给集群中follower的日志index
   	matchIndex      []int    //每个server已知的已复制最新的logentry
   	applyCh			chan ApplyMsg
   
   	//3B
   	lastSnapshotIndex  int  //最近一次snapshot的最后一个日志位置
   	lastSnapshotTerm   int  //最近一次snapshot的最后一个日志的term
   	snapshot           []byte  //数据库快照
   }
   ```

   

2. LogEntry

   ```go
   //日志结构
   type LogEntry struct {
   	Term 			int			//当前轮次
   	Command         interface{} //leader收到的命令
   }
   ```

   

3. ApplyMsg

   ```go
   //raft server 发送给service层的massage格式
   type ApplyMsg struct {
       CommandValid bool			//true:新提交的日志  ， false:同步snapshot
   	Command      interface{}    
   	CommandIndex int
   	SnapShot     PackedSnapShot  //快照
   }
   ```

   

4. packedSnapshot

   ```go
   //数据库快照结构
   type PackedSnapShot struct {
   	Database       map[string]string
   	Index          int
   	LatestSeq      map[int64] int //最近处理的服务器id对应的seqId
   	LatestReply    map[int64] string
   }
   ```

   

### Leader选举

投票原则

- 如果 args.term < rf.term，回复false
- 如果本轮没有投票，并且candidate 的log和收到请求投票的server一样新或者更新，则发出投票



如何保证只有一个leader:

- candidate需要得到半数以上的投票才能当选
- 每一次发起选举都需要开启新一轮的term，candidator收到心跳后自动退出选举
- 在集群出现分区，各自有一个leader的情况下，分区通信恢复，leader根据收到心跳日志的新旧程度选择是否退出

代码

发起选举

```go
//开启选举流程
func (rf *Raft) startElection(cond *sync.Cond) {
	for  {
		rf.mu.Lock()
		cond.Wait()   //等待心跳超时唤醒选举
		DPrintf("server %d start election,term is %d",rf.me,rf.term+1)
        //1.将自己变成candidate,设定选举超时时间（随机），初始化状态等
		rf.serverState = Candidate
		rf.electionTimeout = HEARTBEAT_TIMEOUT + 50 + rand.Int()%150 //150-300ms
		rf.term = rf.term + 1
		rf.voteFor = rf.me
		rf.heartbeatTime =time.Now()
		rf.persist()
		rf.mu.Unlock()
		count := 1
		finish := 0
        
		//2.并发给其他服务器发送投票请求
		cn := make(chan int)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
            //开启子线程发送投票请求
			go func(x int) {
				args := RequestVoteArgs{
					CandidateId: rf.me,
					Term: rf.term,
					EntryIndex: rf.getLastIndex(),
					LastSnapShotIndex: rf.lastSnapshotIndex,
				}
				if len(rf.logs) == 0 {
					args.EntryTerm = rf.lastSnapshotTerm
				}else {
					args.EntryTerm = rf.logs[len(rf.logs)-1].Term
				}
				reply := RequestVoteReply{}
				DPrintf("%d send vote  request to %d ,term is %d",rf.me,x,rf.term)
				ok := rf.sendRequestVote(x,&args,&reply)
				DPrintf("%d get reply vote is %v , term is %d",rf.me,reply.VoteGuarantee,rf.term)
				rf.mu.Lock()
                //发送成功
                //1.如果收到对方投票，计票数加一
                //2.如果收到回复term大于自己的term，跟进term,退出选举
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
                //不管rpc是否返回，记录发送投票请求次数
				finish++
				rf.mu.Unlock()
				cn<- 1
			}(i)
		}
		//4.接收投票并统计 
		for rf.serverState == Candidate {
			//如果收到半数以上的票数，当选为leader
			<-cn
			//DPrintf("channel 数据 %d ",data)
			rf.mu.Lock()
            //如果票数过半，成为leader，并且唤醒发送心跳协程，将nextIndex初始化为自身日志末尾
			if count > len(rf.peers)/2 {
				DPrintf("leader is %d , term is %d",rf.me,rf.term)
				rf.serverState = Leader
				//初始化nextindex
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.getLastIndex() + 1
				}
				//如果当选leader，周期发送心跳验证
				cond.Broadcast()
				rf.mu.Unlock()
				break
			}
			//如果向所有节点发送请求，但没有leader，发起新一轮选举
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
```



处理投票请求

```go
//处理投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//判断自己当前term是否投过票
	//election restriction : 判断自己的logentry term是否比对方大
	DPrintf("%d 收到投票请求，term is %d ,rf.term = %d",rf.me,args.Term,rf.term)

	reply.VoteGuarantee = true

	rf.mu.Lock()
    //本轮是否已投票
	if args.Term == rf.term && rf.voteFor != -1 {
		DPrintf("%d 已经投过票给 %d ,term=%d",rf.me,rf.voteFor,args.Term)
		reply.VoteGuarantee = false
	}else if args.Term < rf.term {  //收到请求的term小于自身的term
		DPrintf("args.term < rf.term")
		reply.VoteGuarantee = false
	}else if rf.lastSnapshotIndex > args.LastSnapShotIndex{   //自身应用的snapshotIndex大于请求的，表示自身的日志更新
		DPrintf("args.LastSnapShotIndex < rf.lastsnapshotindex")
		reply.VoteGuarantee = false
	} else if len(rf.logs) > 0{
        //如果还有未压缩日志，日志是否比发送请求方更新
		if args.EntryTerm < rf.logs[len(rf.logs)-1].Term{
			reply.VoteGuarantee = false
		}else if args.EntryTerm == rf.logs[len(rf.logs)-1].Term && args.EntryIndex < rf.getLastIndex() {
			reply.VoteGuarantee = false
		}
	}
	//设置状态
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
	//更新term
	if rf.term <args.Term {
		rf.term = args.Term
	}

	reply.Term = rf.term
	rf.mu.Unlock()
}
```



### 日志同步

日志同步原则：

- 日志只能由leader 发送给follower
- leader发送日志时带上前一个日志的term和index，follower检查已存在日志和leader发送的prelogterm和prelogindex是否一致
- 已经commit的日志不会被覆盖
- 当大部分follower收到并且添加日志后，leader才commit日志，并且在下次发送心跳时follower会commit日志



日志冲突处理方法：

-  Leader 每次都将 nextIndex – 1，直到成功匹配为止（原始方法）
- （FastBackup）在 AppendEntries Reply 中额外返回 `conflictIndex` 和 `conflictTerm` 两个字段
  1. 如果 prevLogIndex 超过了自己的 Log Index，那么直接返回 conflictIndex = lastLogIndex + 1，conflictTerm = None
  2. 如果 prevLogIndex 在自己 Log 中存在，但是 Term 不等于 prevLogTerm，说明之前有 Leader 同步了 Log，但没来得及提交就 Crash 了，那么 conflictTerm 设为自己 log 中 prevLogIndex 对应的日志的 Term，并且将 conflictIndex 设置为那个 Term 的第一个日志的 Index
- Leader 收到 AppendEntries 失败的时候，根据 `conflictIndex` 和 `conflictTerm` 两个字段来找到最后匹配的日志。
  1. 首先找 conflictTerm 在自己日志中是否存在，存在的话将 nextIndex 设置为那个 Term 最后一个 Log 的 Index + 1
  2. 如果找不到，那么 nextIndex = conflictIndex



### 安全性

为了防止已提交的日志被更改，raft采用以下措施：

- 拥有最新的已提交的log entry的Follower才有资格成为Leader
- Leader只能推进commit index来提交当前term的已经复制到大多数服务器上的日志，旧term日志的提交要等到提交当前term的日志来间接提交（log index 小于 commit index的日志被间接提交）。即 **提交日志的term必须等于当前term**
- Leader提交日志的index应当小于等于(存在n,使得大多数matchIndex[x]>=n)，即大多数节点已复制的日志index



### 状态持久化

为防止服务器节点崩溃重启后丢失已有日志

需要持久化的属性：

```go
Term := rf.term
VoteFor := rf.voteFor
Logs := rf.logs
LastSnapshotInd := rf.lastSnapshotIndex
LastSnapshotTerm := rf.lastSnapshotTerm
```





### KVservice

结构：

​		每个节点由一个KVservice和raftServer组成，组成一个集群，与客户端通信。



客户端通信原则：

- 客户端发送命令到服务端，通过对每一个服务器轮询，若服务器返回自己不是leader，则尝试下一个服务器
- 若服务器超时不回复，尝试下一个服务器



服务层：

- 服务层接收客户端的命令，并且将命令发送给faftServer，等待raftServer同步日志完成后，再应用命令，返回结果
- 服务层判断自己是否是leader，如果不是，直接返回



raft层：

- 接收命令并同步
- 同时实现日志压缩、数据库快照和状态持久化



如何解决客户端因网络问题的重复请求，过期请求

- 客户端发送请求时附加上当前客户端编号和请求序列号
- Server层维护一个客户端编号和最新处理请求序列号的Map，如果收到的序列号不大于已处理的序列号，则丢弃该请求



Server层代码结构

```go
//Service的结构体定义
type KVServer struct {
   mu      sync.Mutex
   me      int
   rf      *raft.Raft
   applyCh chan raft.ApplyMsg
   dead    int32 // set by Kill()

   maxraftstate int // snapshot if log grows this big

   // Your definitions here.
   dataBase         map[string]string //存储键值对
   notify           map[int]chan packedReply
   //latestProcessSeq map[int64]*packedReply //最近处理的服务器id对应的seqId和返回值
   latestSeq        map[int64]int
   latestReply      map[int64]string
   lastappliedIndex int
}
```

Get和putAppend处理客户端请求

```go
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
   //判断自己是否是leader
   //DPrintf("get in...")
   if _, isLeader := kv.rf.GetState(); !isLeader {
      //reply.IsLeader = false
      reply.Err = ErrWrongLeader
      return
   }
   //防止重复发送的命令
   //重复命令的seqId相同
   kv.mu.Lock()
   if seq, ok := kv.latestSeq[args.ClientId]; ok {
      if args.SeqId <= seq{
         //返回相应的值，因为之前的值没有到达客户端，通道被销毁了，肯定被丢弃了
         reply.Value = kv.latestReply[args.ClientId]
         reply.Err = OK
         DPrintf("client %d 重复请求,get key%s,value %s",args.ClientId,args.Key,reply.Value)
         //reply.IsLeader = packedReply.isLeader
         kv.mu.Unlock()
         return
      }
   }
   kv.mu.Unlock()

   //将操作加入日志等待commit成功后返回
   command := Op{
      Opreation: GetOp,
      Key:       args.Key,
      ClientId:  args.ClientId,
      SeqId:     args.SeqId,
   }
   kv.mu.Lock()
   index, _, isLeader := kv.rf.Start(command)
   kv.mu.Unlock()
   if !isLeader {
      //reply.IsLeader = false
      reply.Err = ErrWrongLeader
      return
   }

   DPrintf("get index = %d,key = %s", index, args.Key)
   //阻塞等待线程监控是否commit完成
   kv.mu.Lock()
   kv.notify[index] = make(chan packedReply)
   kv.mu.Unlock()
   r := <-kv.notify[index]
   DPrintf("client %d get value %s",args.ClientId,r.Value)
   if _, isleader2 := kv.rf.GetState();!isleader2{
      reply.Err = ErrWrongLeader
      return
   }
   reply.Value = r.Value
   reply.Err = r.Err
}
```



watchingCommitment

watchingCommitment协程用于监听raft层的日志同步结果，如果当前日志已被同步到大部分节点，则leader会通知service层返回结果给客户端

```go
//监控日志是否复制到大部分服务器，如果是，leader会发送通知到applyCh中
func (kv *KVServer) watchingCommitment(cond *sync.Cond) {
   for entries := range kv.applyCh {
      //commit snapshot时CommandValid为false
      if kv.killed() {
         DPrintf("%d服务器killed",kv.me)
         cond.Broadcast()
         break
      }
      if !entries.CommandValid && entries.SnapShot.Index > kv.lastappliedIndex{
         kv.dataBase = entries.SnapShot.Database
         kv.latestSeq = entries.SnapShot.LatestSeq
         kv.latestReply = entries.SnapShot.LatestReply
         DPrintf("server %d db is %v",kv.me,kv.dataBase)
         continue
      }
      //拿到已经提交的命令
      var value string
      var err Err
      command := entries.Command.(Op)


      DPrintf("commit client id %d，opretion is %s", command.ClientId,command.Opreation)
      seqId,ok := kv.latestSeq[command.ClientId]
      if !ok || seqId < command.SeqId {
         kv.mu.Lock()
         kv.latestSeq[command.ClientId] = command.SeqId
         switch command.Opreation {
         case GetOp:
            if v, ok := kv.dataBase[command.Key]; ok {
               value = v
               err = OK
            } else {
               value = ""
               err = ErrNoKey
            }
            kv.latestReply[command.ClientId] = value
            break
         case PutOp:
            kv.dataBase[command.Key] = command.Value
            err = OK
            kv.latestReply[command.ClientId] = command.Value
            break
         case AppendOp:
            kv.dataBase[command.Key] += command.Value
            err = OK
            kv.latestReply[command.ClientId] = kv.dataBase[command.Key]
            break
         default:
            log.Fatal("无效的命令")
         }
         //latestreply.Err = err
         seqId = command.SeqId
         kv.mu.Unlock()
      }else {
         kv.mu.Lock()
         value = kv.latestReply[command.ClientId]
         err = OK
         //p = *kv.latestProcessSeq[command.ClientId]
         //p = *latestreply
         kv.mu.Unlock()
      }

      p := packedReply{
         //seqId: seqId,
         Value: value,
         Err: err,
      }

      //通知处理线程返回客户端消息
      DPrintf("log commit,index is %d,command is %v", entries.CommandIndex, entries.Command)
      //判断通道是否存在并关闭，假如不关闭，follower提交的日志也会发送，但接受函数已返回，导致阻塞
      kv.mu.Lock()
      if channel, ok := kv.notify[entries.CommandIndex]; ok && channel != nil {
         DPrintf("%d notify index %d", kv.me, entries.CommandIndex)
         kv.lastappliedIndex = entries.CommandIndex
         channel <- p
         close(channel)
         delete(kv.notify, entries.CommandIndex)
         cond.Broadcast()
      }
      kv.mu.Unlock()
   }
}
```



testLoglenLoop

testLoglenLoop协程检测当前日志长度是否超限，需要压缩日志，每次leader提交日志时唤醒该线程检测。

```go
//检测日志是否超上限
func (kv *KVServer)testLoglenLoop(cond *sync.Cond){
   var latestSeq        map[int64]int
   var latestReply      map[int64]string
   var lastappliedIndex int
   for  {
      kv.mu.Lock()
      cond.Wait()
      kv.mu.Unlock()
      if kv.killed() {
         DPrintf("%dsever退出",kv.me)
         break
      }
      var dataBase         map[string]string //存储键值对
      func() {
         _, isleader := kv.rf.GetState()
         if isleader && kv.maxraftstate != -1 && kv.rf.GetLogSize() >= kv.maxraftstate {
            //发送snapshot,index
            DPrintf("%d发送snapshot,日志长度%d", kv.me, kv.rf.GetLogSize())
            kv.mu.Lock()
            dataBase = kv.dataBase
            lastappliedIndex = kv.lastappliedIndex
            latestSeq = kv.latestSeq
            latestReply = kv.latestReply
            kv.mu.Unlock()
         }
      }()
      if dataBase != nil {
         kv.rf.Snapshot(dataBase, lastappliedIndex, latestSeq, latestReply)
      }
      //time.Sleep(50*time.Millisecond)
   }
}
```



### 日志压缩

日志压缩包含数据库快照和状态持久化，压缩后的日志由压缩时间点的数据库快照和后续日志组成。

日志压缩原因：

- 服务器长时间运行可以导致日志无限增长，会占用大量存储时间
- 服务器重启回复到之前的状态需要回放所有日志，占用大量时间



Service层发送给Raft的snapshot内容包括

```go
dataBase = kv.dataBase   //当前数据库数据
lastappliedIndex = kv.lastappliedIndex   //leader最后提交的log index
latestSeq = kv.latestSeq    //服务器最后回复的请求序列号
latestReply = kv.latestReply  //服务器最后回复的请求内容
//latestSeq和latestReply是以客户端编号为key的map
latestSeq        map[int64]int
latestReply      map[int64]string
```



Leader收到snapshot命令后，对日志进行截断并且持久化当前状态

```go
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
   rf.mu.Lock()
   defer rf.mu.Unlock()
   //从-1开始，表示snapshot包含的最后一个日志index
   rf.lastSnapshotTerm = rf.logs[rf.leaderIndex2logpos(index)].Term
   leftLogs := make([]LogEntry, rf.getLastIndex()-index)
   copy(leftLogs,rf.logs[rf.leaderIndex2logpos(index)+1 :])
   rf.logs = leftLogs
   DPrintf("上一次index =%d ,lastSnapshotIndex = %d,日志长度%d",rf.lastSnapshotIndex,index, len(rf.logs))
   rf.lastSnapshotIndex = index
   //序列化
   w1 := new(bytes.Buffer)
   e1 := labgob.NewEncoder(w1)
   e1.Encode(snapShot)
   data1 := w1.Bytes()
   rf.snapshot = data1
   rf.saveSnapshot(data1)
}
```



leader发送snapshot的时机

在日志压缩引入之前，leader每次心跳倒计时结束发送日志给follower同步

引入日志压缩之后，对日志同步函数进行改造，原则是：

```
每一次发送日志都附带上snapshot,由follower判断是否应用。
原则：如果snapshotindex相等，忽略snapshot,走后面logreplicate流程
如果snapshot大于leadersnapshot,忽略直接返回
如果小于，应用，将conflictindex移到现存日志开始处
```



同时对AppendEntries函数进行改造

在之前的日志判断之前先判断是否需要应用快照。

```go
//判断快照
//如果leader快照不如自己的新，则放弃
if args.LastSnapshotIndex < rf.lastSnapshotIndex {
   reply.IsSuccess = false
   return
}else if args.LastSnapshotIndex == rf.lastSnapshotIndex{

} else {
   //快照日志截取的位置比现有日志短
   lastIndex := rf.getLastIndex()   //现有日志末尾的相对坐标
   if args.LastSnapshotIndex < lastIndex {
      //和leader的lastsnapshotterm有冲突，舍弃所有日志
      if rf.logs[rf.leaderIndex2logpos(args.LastSnapshotIndex)].Term != args.LastSnapshotTerm {
         rf.logs = make([]LogEntry,0)

      }else {
          //日志没有冲突，应用snapshot和日志
         leftLogs := make([]LogEntry, lastIndex-args.LastSnapshotIndex)
         copy(leftLogs,rf.logs[rf.leaderIndex2logpos(args.LastSnapshotIndex+1) :])
         rf.logs = leftLogs
      }
   }else {
      //快照位置比现有的日志末尾相对index要大，意味着现有日志全部落在快照之内，则清空日志
      rf.logs = make([]LogEntry,0)
   }
    //状态持久化，并且将snapshot发送到Service层执行
   rf.lastSnapshotIndex = args.LastSnapshotIndex
   rf.lastSnapshotTerm = args.LastSnapshotTerm
   rf.lastApplied = args.LastSnapshotIndex
   rf.snapshot = args.SnapShotBytes
   rf.saveSnapshot(args.SnapShotBytes)
   go rf.installSnapshotToServer(args.SnapShotBytes)
}
```



坐标改造

- 由于应用日志压缩，导致不能简单依靠现存日志的下标位置判断日志新旧
- 因此引入全局的日志坐标，包括之前已经压缩的日志长度，对现有坐标进行重新计算



如何保证线性一致性**（强一致性）**

- 一致性写：

  - raft的日志同步再提交，应用后再返回就保证了一致性写
- 一致性读：

  - **Raft log read** :像写操作一样处理读操作
  - **Read Index**:主要思想为收到读请求时记录当前的commit index作为read index，只有数据库将日志应用到大于等于read index时才返回读取结果。
  
    1. Leader 在收到客户端读请求时，记录下当前的 commit index，称之为 read index
    2. Leader 向 followers 发起一次心跳包，这一步是为了确保领导权，避免网络分区时少数派 leader 仍处理请求
    3. 等待状态机**至少**应用到 read index（即 apply index **大于等于** read index）
    4. 执行读请求，将状态机中的结果返回给客户端。
  - **Lease Read**: leader 设置一个**比选举超时（Election Timeout）更短的时间作为租期**，在租期内我们可以相信其它节点一定没有发起选举，集群也就一定不会存在脑裂，所以在这个时间段内我们直接读主即可，而非该时间段内可以继续走 Read Index 流程，Read Index 的心跳包也可以为租期带来更新。

    - 租期设置：leader 发送 heartbeat 的时候，会首先记录一个时间点 start，当系统大部分节点都回复了 heartbeat response，那么我们就可以认为 leader 的 lease 有效期可以到 start + election timeout / clock drift bound 这个时间点。
    - 缺点：如果节点之间的cpu 时钟频率相差太大，就无法保证一致性。
  - **Follower Read**:Follower 在收到客户端的读请求时，向 leader 询问当前最新的 commit index，反正所有日志条目最终一定会被同步到自己身上，follower 只需等待该日志被自己 commit 并 apply 到状态机后，返回给客户端本地状态机的结果即可



### 成员变更（未实现）

替换或增减集群成员时需要考虑成员变更的方案。

raft解决方案：

- 将集群配置文件作为特殊日志在集群内进行同步
- 问题：**所有将集群从旧配置直接完全切换到新配置的方案都是不安全的**
  - 日志同步存在延迟，集群成员不可能同时原子地切换其成员配置
  - 可能造成多个leader的脑裂问题



#### 两阶段切换集群成员配置

**单节点增减**

单节点增减不会影响集群安全性。

**多节点增减**

raft增加了过渡状态  **joint consensus**

旧配置 cold   新配置  cnew   过渡配置  cold-new

**规则**

joint consensus状态下：

- 日志被提交给新老配置下所有的节点
- 新旧配置中所有机器都可能称为Leader
- 达成一致（选举和提交）要在两种配置上获得超过半数的支持



阶段一：

- 客户端将 C-new 发送给 leader，leader 将 C-old 与 C-new 取**并集**并立即apply，我们表示为 **C-old,new**。
- Leader 将 C-old,new 包装为日志同步给其它节点。
- Follower 收到 C-old,new 后立即 apply，当 **C-old,new 的大多数节点（即 C-old 的大多数节点和 C-new 的大多数节点）**都切换后，leader 将该日志 commit。

阶段二：

- Leader 接着将 C-new 包装为日志同步给其它节点。

  Follower 收到 C-new 后立即 apply，如果此时发现自己不在 C-new 列表，则主动退出集群。

  Leader 确认 **C-new 的大多数节点**都切换成功后，给客户端发送执行成功的响应。



**问题**

- 新加入的节点需要大量时间追赶日志，影响集群对外服务
  - Raft采用以下机制来保证可用性： 新加入节点没有投票权，直到追上日志
- 如果当前Leader是一个要被下线的节点
  - 在C-old-new的状态下，Leader依旧可用；在C-new被commit之后Leader实际已经从集群中脱离
- 节点由于网络隔离或者被踢出集群时会不断发起选举增加Term，导致影响集群正常工作
  - 增加**预选举机制**
    - 一个follower节点选举超时成为candidate后，不会将自身的Term自增，而是先去询问集群中的其他节点自己的日志进展是否能让自己成为leader，如果是那么预选举成功，该节点自增Term，然后走正常的选举流程，否则预选举失败，该节点重新成为follower。

