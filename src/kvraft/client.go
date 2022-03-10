package kvraft

import (
	"6.824-golabs-2020/src/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader  int  //上一次请求知道的leader
	clientId    int64
	seqId       int
	mu          sync.Mutex
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
	ck.seqId = 0
	ck.clientId = nrand()
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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//调用server.get RPC
	//防止get请求发送到处于小部分分区的服务器中返回错误结果，
	//一个简单的解决办法是将get请求加入日志，等到commit成功之后再返回
	out := ""

	args := GetArgs{
		Key: key,
		ClientId: ck.clientId,
		SeqId: ck.seqId,
	}
	reply := GetReply{}
	cn := make(chan bool)
	i :=ck.lastLeader
	for  {
			go func(x int) {
				DPrintf("send get to server %d",x)
				ok := ck.servers[x].Call("KVServer.Get", &args, &reply)
				cn<-ok
			}(i)
			//超时，尝试下一个server
			select {
			case isOk := <-cn:
				if isOk && reply.Err != ErrWrongLeader{
					DPrintf("getvalue %s ,key %s",reply.Value,key)
					out = reply.Value
					ck.lastLeader = i
					ck.seqId++
					return out
				}
			case <-time.After(600*time.Millisecond):
				DPrintf("请求服务器%d超时",i)
			}

			i++
			i = i% len(ck.servers)
	}
	return out
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

	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		SeqId: ck.seqId,
	}
	reply := PutAppendReply{}
	i := ck.lastLeader
	for  {
		cn := make(chan bool)
			go func(x int) {
				DPrintf("send putappend to server %d ，key is %s ,v is %s ,seq is %d",x,key,value,ck.seqId)
				ok := ck.servers[x].Call("KVServer.PutAppend", &args, &reply)
				if cn != nil {
					cn<-ok
				}
			}(i)
			//超时，尝试下一个server
			select {
			case isOk := <-cn:
				if reply.Err != ErrWrongLeader && isOk{
					DPrintf("返回ok,seq%d",ck.seqId)
					ck.lastLeader = i
					ck.seqId++
					return
				}
			case <-time.After(1000*time.Millisecond):
				DPrintf("请求服务器%d超时",i)
			}

			i++
			i = i% len(ck.servers)
	}


}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
