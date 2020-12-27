package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Read     bool
	Append   bool
	Serverid int
	Uniqid   int64
	Key      string
	Value    string
	ReplyCh  chan int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	rfisleader bool
	rfterm     int
	result     map[string]string
	applidID   map[int64]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// read from local map
	cmd := kv.generateCmd(args, true)
	_, term, isleader := kv.raftStart(cmd)

	if isleader {
		log.Printf("[%v] (Get) is leader. start to check apply state", kv.me)
	loop:
		for {
			select {
			case server := <-cmd.ReplyCh:
				{
					log.Printf("[%v] (Get) apply success, recieve server id: %v",
						kv.me, server)
					reply.IsLeader = true
					reply.Success = true
					reply.Value = kv.readKey(args.Key)
					break loop
				}
			default:
				{
					currentterm, currentisleader := kv.getCurrentState()
					if currentterm != term {
						log.Printf("[%v] (Get) not a leader any more. current term %v. isleader: %v",
							kv.me, currentterm, currentisleader)
						reply.IsLeader = false
						reply.Success = false
						break loop
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	} else {
		log.Printf("[%v] (Get) not a leader.", kv.me)
		reply.IsLeader = false
		reply.Success = false
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// put the key+value to raft
	// And statu machine can get and append it
	cmd := kv.generateCmd(args, false)
	index, term, isleader := kv.raftStart(cmd)

	if isleader {
		log.Printf("[%v] (Put/Append) is leader. start to check apply state", kv.me)
		log.Printf("[%v] write info - <%v, %v> k: %v, v: %v, op: %v",
			kv.me, index, term, args.Key, args.Value, args.Op)
	loop:
		for {
			select {
			case server := <-cmd.ReplyCh:
				{
					log.Printf("[%v] (Put/Append) apply success. recieve server id: %v",
						kv.me, server)
					if server == kv.me {
						reply.IsLeader = true
						reply.Success = true
						break loop
					}
				}
			default:
				{
					currentterm, currentisleader := kv.getCurrentState()
					if currentterm != term {
						log.Printf("[%v] (Put/Append) not a leader any more. current term %v. isleader: %v",
							kv.me, currentterm, currentisleader)
						reply.IsLeader = false
						reply.Success = false
						break loop
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	} else {
		log.Printf("[%v] (Put/Append) not a leader.", kv.me)
		reply.IsLeader = false
		reply.Success = false
	}
}

func (kv *KVServer) raftStart(cmd interface{}) (int, int, bool) {
	index, term, isleader := kv.rf.Start(cmd)
	kv.updateState(term, isleader)

	return index, term, isleader
}

func (kv *KVServer) generateCmd(args interface{}, get bool) Op {
	log.Printf("[%v] generate log. get: %v", kv.me, get)
	ret := Op{}
	ch := make(chan int, 100)
	ret.ReplyCh = ch
	ret.Serverid = kv.me
	if get { // generate get cmd
		a := args.(*GetArgs)
		ret.Uniqid = a.Uniqid
		ret.Read = true
	} else { // generate append/put cmd
		a := args.(*PutAppendArgs)
		ret.Uniqid = a.Uniqid
		ret.Read = false
		ret.Key = a.Key
		ret.Value = a.Value
		if a.Op == "Put" {
			ret.Append = false
		} else {
			ret.Append = true
		}
	}

	return ret
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.result = make(map[string]string)
	kv.applidID = make(map[int64]bool)
	go kv.applyRoutine(kv.applyCh)
	go kv.updateRaftStateRoutine()

	return kv
}

func (kv *KVServer) readKey(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ret, _ := kv.result[key]
	return ret
}

func (kv *KVServer) addOrAppendKeyValue(key string, value string, append bool, uid int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.applidID[uid] {
		log.Printf("[%v] uid %v has been added into applied id map. skip append",
			kv.me, uid)
	} else {
		if append {
			kv.result[key] += value
		} else {
			kv.result[key] = value
		}
		kv.applidID[uid] = true
	}
}

func (kv *KVServer) applyRoutine(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			if !cmd.Read {
				log.Printf("[%v] Put/Append apply info - key: %v, value: %v, append: %v, index: %v",
					kv.me, cmd.Key, cmd.Value, cmd.Append, msg.CommandIndex)
				kv.addOrAppendKeyValue(cmd.Key, cmd.Value, cmd.Append, cmd.Uniqid)
			}
			term, _ := kv.getCurrentState()
			if (cmd.Serverid == kv.me) && (msg.CommandTerm == term) {
				cmd.ReplyCh <- kv.me
			}
		}
	}
}

func (kv *KVServer) updateRaftStateRoutine() {
	for {
		term, isleader := kv.rf.GetState()
		kv.updateState(term, isleader)

		time.Sleep(200 * time.Microsecond)
	}
}

func (kv *KVServer) updateState(term int, isleader bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if term >= kv.rfterm {
		kv.rfterm = term
		if isleader {
			kv.rfisleader = isleader
		}
	}
}

func (kv *KVServer) getCurrentState() (int, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	term := kv.rfterm
	isleader := kv.rfisleader

	return term, isleader
}
