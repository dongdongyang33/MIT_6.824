package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	servernum     int
	currentLeader int
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
	ck.servernum = len(ck.servers)
	ck.currentLeader = 0
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
	args := GetArgs{}
	args.Uniqid = nrand()
	args.Key = key
	reply := GetReply{}

	for {
		id := (ck.currentLeader) % ck.servernum
		ok := ck.servers[id].Call("KVServer.Get", &args, &reply)
		if !ok || !reply.IsLeader {
			log.Printf("Lost connect with server %v or it is not a leader (%v), retry...",
				id, reply.IsLeader)
			ck.currentLeader++
			time.Sleep(5 * time.Millisecond)
		} else {
			if reply.Success {
				log.Printf("Get success with server %v! get value = %v", id, reply.Value)
				ck.currentLeader = id
				break
			}
		}
	}

	// You will have to modify this function.
	return reply.Value
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
	args := PutAppendArgs{}
	args.Uniqid = nrand()
	args.Key = key
	args.Value = value
	args.Op = op
	reply := PutAppendReply{}

	for {
		id := (ck.currentLeader) % ck.servernum
		ok := ck.servers[id].Call("KVServer.PutAppend", &args, &reply)
		if !ok || !reply.IsLeader {
			log.Printf("Lost connect with server %v or it is not a leader (%v), retry...",
				ck.currentLeader, reply.IsLeader)
			ck.currentLeader++
			time.Sleep(1 * time.Millisecond)
		} else {
			if reply.Success {
				log.Printf("Put/Append success with server %v!", id)
				ck.currentLeader = id
				break
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
