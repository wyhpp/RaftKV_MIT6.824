package kvraft

import (
	"6.824-golabs-2020/src/labgob"
	"6.824-golabs-2020/src/labrpc"
	"6.824-golabs-2020/src/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType string

const (
	GetOp    OpType = "Get"
	PutOp    OpType = "Put"
	AppendOp OpType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Opreation OpType
	Key       string
	Value     string
	ClientId  int64
	SeqId     int
}

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
	latestProcessSeq map[int64]*packedReply //最近处理的服务器id对应的seqId
}

type packedReply struct {
	seqId    int
	Value    string
	Err      Err
	isLeader bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//判断自己是否是leader
	DPrintf("get in...")
	if _, isLeader := kv.rf.GetState(); !isLeader {
		//reply.IsLeader = false
		reply.Err = ErrWrongLeader
		return
	}
	//防止重复发送的命令
	//重复命令的seqId相同
	kv.mu.Lock()
	if latestreply, ok := kv.latestProcessSeq[args.ClientId]; ok {
		if args.SeqId <= latestreply.seqId{
			//返回相应的值，因为之前的值没有到达客户端，通道被销毁了，肯定被丢弃了
			reply.Value = latestreply.Value
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
	//保存seqid信息
	//p := packedReply{
	//	isLeader: isLeader,
	//	seqId:    args.SeqId,
	//}

	//kv.latestProcessSeq[args.ClientId] = args.SeqId

	DPrintf("get index = %d,key = %s", index, args.Key)
	//阻塞等待线程监控是否commit完成
	r := <-kv.notify[index]
	DPrintf("client %d get value %s",args.ClientId,r.Value)
	reply.Value = r.Value
	reply.Err = r.Err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//判断自己是否是leader
	DPrintf("putappend in...")
	if _, isLeader := kv.rf.GetState(); !isLeader {
		//reply.IsLeader = false
		reply.Err = ErrWrongLeader
		return
	}
	//重复命令的seqId相同
	kv.mu.Lock()
	if latestreply, ok := kv.latestProcessSeq[args.ClientId]; ok {
		if args.SeqId <= latestreply.seqId {
			//返回相应的值，因为之前的值没有到达客户端，通道被销毁了，肯定被丢弃了
			reply.Err = OK
			//reply.IsLeader = packedReply.isLeader
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//将操作加入日志等待commit成功后返回
	command := Op{
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	if args.Op == "Put" {
		command.Opreation = PutOp
	} else {
		command.Opreation = AppendOp
	}
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	kv.mu.Unlock()
	if !isLeader {
		//reply.IsLeader = false
		reply.Err = ErrWrongLeader
		return
	}

	//保存seqid信息
	//p := packedReply{
	//	isLeader: isLeader,
	//	seqId:    args.SeqId,
	//}

	DPrintf("putappend client id %d", args.ClientId)
	//kv.latestProcessSeq[args.ClientId] = args.SeqId
	//阻塞等待线程监控是否commit完成
	DPrintf("putappend index = %d,command = {%s,%s}", index, args.Key, args.Value)
	r := <-kv.notify[index]
	reply.Err = r.Err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.latestProcessSeq = make(map[int64]*packedReply)
	kv.notify = make(map[int]chan packedReply)
	kv.dataBase = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.watchingCommitment()

	return kv
}

//监控日志是否复制到大部分服务器，如果是，leader会发送通知到applyCh中
func (kv *KVServer) watchingCommitment() {
	for entries := range kv.applyCh {
		//commit snapshot时CommandValid为false
		if !entries.CommandValid {
			continue
		}
		//拿到已经提交的命令
		var value string
		var err Err
		command := entries.Command.(Op)
		p := packedReply{}

		DPrintf("commit client id %d", command.ClientId)
		if latestreply,ok := kv.latestProcessSeq[command.ClientId];
		!ok || latestreply.seqId < command.SeqId {
			kv.mu.Lock()
			if !ok {
				kv.latestProcessSeq[command.ClientId] = new(packedReply)
				latestreply = kv.latestProcessSeq[command.ClientId]
			}
			switch command.Opreation {
			case GetOp:
				if v, ok := kv.dataBase[command.Key]; ok {
					value = v
					err = OK
				} else {
					value = ""
					err = ErrNoKey
				}
				latestreply.Value = value
				break
			case PutOp:
				kv.dataBase[command.Key] = command.Value
				err = OK
				latestreply.Value = kv.dataBase[command.Key]
				break
			case AppendOp:
				kv.dataBase[command.Key] += command.Value
				err = OK
				latestreply.Value = kv.dataBase[command.Key]
				break
			default:
				log.Fatal("无效的命令")
			}
			latestreply.Err = err
			latestreply.seqId = command.SeqId
			//kv.latestProcessSeq[command.ClientId] = &p
			p = *latestreply
			kv.mu.Unlock()
		}else {
			kv.mu.Lock()
			//value = kv.dataBase[command.Key]
			err = OK
			//p = *kv.latestProcessSeq[command.ClientId]
			p = *latestreply
			kv.mu.Unlock()
		}

		//通知处理线程返回客户端消息
		DPrintf("log commit,index is %d,command is %v", entries.CommandIndex, entries.Command)
		//判断通道是否存在并关闭，假如不关闭，follower提交的日志也会发送，但接受函数已返回，导致阻塞
		if channel, ok := kv.notify[entries.CommandIndex]; ok && channel != nil {
			DPrintf("%d notify index %d", kv.me, entries.CommandIndex)
			//日志长度大于maxraftstate
			if kv.rf.GetLogSize() >= kv.maxraftstate {
				//发送snapshot,index
				dataBase := kv.dataBase
				index := entries.CommandIndex
				kv.rf.Snapshot(dataBase,index)
			}
			kv.mu.Lock()
			channel <- p
			close(channel)
			delete(kv.notify, entries.CommandIndex)
			kv.mu.Unlock()
		}
	}
}
