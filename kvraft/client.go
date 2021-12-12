package raftkv

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers        []*labrpc.ClientEnd
	clientID       int64
	expectedLeader int // the server we think is the leader
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
	ck.clientID = nrand()
	ck.expectedLeader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	commandID := nrand()
	server := ck.expectedLeader

	for {

		args := GetArgs{key, commandID}
		reply := GetReply{}

		f := nrand()
		tn := time.Now()
		fmt.Printf("%v Get begun s:%d k:%s v:%s cmdID:%v\n", f, server, args.Key, "", commandID)

		replyCh := make(chan AsyncRPCReply)

		go ck.asyncRPC(server, replyCh, "RaftKV.Get", &args, &reply)

		// get timer for a heartbeat interval
		heartbeatTimer := time.NewTimer(time.Second)

		select {
		// if heartbeat times out
		case <-heartbeatTimer.C:
			// next server
			server = (server + 1) % len(ck.servers)
			fmt.Println(f, " timed out (forced) after ", time.Since(tn))

		case rpcReply := <-replyCh:

			reply, ok := rpcReply.reply.(*GetReply), rpcReply.ok

			if !ok {
				// ok timeout
				server = (server + 1) % len(ck.servers)
				fmt.Println(f, " timed out after ", time.Since(tn))
			} else if reply.WrongLeader {
				server = (server + 1) % len(ck.servers)
				fmt.Println(f, " WrongLeader after ", time.Since(tn))
			} else if reply.Err != "" {
				server = (server + 1) % len(ck.servers)
				fmt.Println(f, " Err after ", time.Since(tn))
			} else {
				// we have successfully put the value, so
				ck.expectedLeader = server
				fmt.Println(f, " put returned after ", time.Since(tn))
				return reply.Value
			}
		}
	}
}

type AsyncRPCReply struct {
	reply interface{}
	ok    bool
}

func (ck *Clerk) asyncRPC(server int, replyCh chan AsyncRPCReply, rpcName string, args interface{}, reply interface{}) {
	ok := ck.servers[server].Call(rpcName, args, reply)
	replyCh <- AsyncRPCReply{reply, ok}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	commandID := nrand()
	server := ck.expectedLeader

	for {

		args := PutAppendArgs{key, value, op, commandID}
		reply := PutAppendReply{}

		f := nrand()
		tn := time.Now()
		fmt.Printf("%v Put begun s:%d k:%s v:%s cmdID:%v\n", f, server, args.Key, args.Value, commandID)

		replyCh := make(chan AsyncRPCReply)

		go ck.asyncRPC(server, replyCh, "RaftKV.PutAppend", &args, &reply)

		// get timer for a heartbeat interval
		heartbeatTimer := time.NewTimer(time.Second)

		select {
		// if heartbeat times out
		case <-heartbeatTimer.C:
			// next server
			server = (server + 1) % len(ck.servers)
			fmt.Println(f, " timed out (forced) after ", time.Since(tn))

		case rpcReply := <-replyCh:

			fmt.Println("here")

			reply, ok := rpcReply.reply.(*PutAppendReply), rpcReply.ok

			if !ok {
				// ok timeout
				server = (server + 1) % len(ck.servers)
				fmt.Println(f, " timed out after ", time.Since(tn))
			} else if reply.WrongLeader {
				server = (server + 1) % len(ck.servers)
				fmt.Println(f, " WrongLeader after ", time.Since(tn))
			} else if reply.Err != "" {
				server = (server + 1) % len(ck.servers)
				fmt.Println(f, " Err after ", time.Since(tn))
			} else {
				// we have successfully put the value, so
				ck.expectedLeader = server
				fmt.Println(f, " put returned after ", time.Since(tn))
				return
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
