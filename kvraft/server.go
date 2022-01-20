package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpName    string
	Key       string
	Value     string
	CommandID int64
}

type OpRecord struct {
	op              Op
	executionResult string
}

type rpcReply struct {
	result string
	Err    Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	isLeader    bool
	currentTerm int

	// Your definitions here.
	values map[string]string

	// recordOfPendingIntents
	opHistory  map[int64]OpRecord
	waitingOps map[int64]chan rpcReply

	// kill
	stopListenToApplyCh      chan bool
	stopListenToStateChanges chan bool
}

func (kv *RaftKV) executeOperation(op Op) string {
	result := ""
	if op.OpName == "Put" {
		kv.values[op.Key] = op.Value
	} else if op.OpName == "Append" {
		kv.values[op.Key] = kv.values[op.Key] + op.Value
	} else if op.OpName == "Get" {
		result = kv.values[op.Key]
	} else {
		// fmt.Println("WHATTT")
	}
	return result
}

func (kv *RaftKV) notifyToRPC(op Op, result string) {
	replyCh, isPresent := kv.waitingOps[op.CommandID]
	delete(kv.waitingOps, op.CommandID)
	if isPresent {
		// me, name, k, v, cmdID := kv.me, op.OpName, op.Key, op.Value, op.CommandID
		// fmt.Printf("s%d notifToCh[*]: %s, k:%s, v:%s, %v\n", me, name, k, v, cmdID)
		replyCh <- rpcReply{result, ""}
		// fmt.Printf("s%d notifToCh[**]: %s, k:%s, v:%s, %v\n", me, name, k, v, cmdID)
	}
}

func (kv *RaftKV) getOpFromHistory(commandID int64) (string, bool) {
	result, isPresent := kv.opHistory[commandID]
	return result.executionResult, isPresent
}

func (kv *RaftKV) storeInHistory(op Op, result string) {
	kv.opHistory[op.CommandID] = OpRecord{op, result}
}

func (kv *RaftKV) handleApply(log raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// get op
	op := log.Command.(Op)

	// check if op is already executed
	result, isAlreadyExecuted := kv.getOpFromHistory(op.CommandID)
	if !isAlreadyExecuted {
		// if not executed, exectute
		result = kv.executeOperation(op)
		kv.storeInHistory(op, result)
	}
	// kv.printValues(op)

	// if there is a wait for the value
	kv.notifyToRPC(op, result)

	// if state changed, then reply to your channels that there was an error

	// store prev state, and update current state
	wasLeader, prevTerm := kv.isLeader, kv.currentTerm
	kv.currentTerm, kv.isLeader = kv.rf.GetState()

	// if server was a leader earlier, or there has been an increase in term
	if (wasLeader && !kv.isLeader) || kv.currentTerm != prevTerm {
		// clear waiting operations
		for i, replyCh := range kv.waitingOps {
			// fmt.Println("!")
			replyCh <- rpcReply{"", "leader changed!"}
			// fmt.Println("!!")
			delete(kv.waitingOps, i)
		}
	}
}

func (kv *RaftKV) printValues(cmd Op) {
	s := "{"
	for key, value := range kv.values {
		s += key + ": " + value + ","
	}
	s += "}"
	fmt.Println("ApplyCh:", cmd, " values at s", kv.me, "values:", s)
}

func (kv *RaftKV) listenToApplyCh() {
	exit := false
	for {
		select {
		case log := <-kv.applyCh:
			kv.handleApply(log)
		case <-kv.stopListenToApplyCh:
			exit = true
		}

		if exit {
			break
		}
	}
}

func (kv *RaftKV) handleStateChange() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if state changed, then reply to your channels that there was an error

	// store prev state, and update current state
	wasLeader, prevTerm := kv.isLeader, kv.currentTerm
	kv.currentTerm, kv.isLeader = kv.rf.GetState()

	// if server was a leader earlier, or there has been an increase in term
	if (wasLeader && !kv.isLeader) || kv.currentTerm != prevTerm {
		// clear waiting operations
		for i, replyCh := range kv.waitingOps {
			// fmt.Println("!")
			replyCh <- rpcReply{"", "leader changed!"}
			// fmt.Println("!!")
			delete(kv.waitingOps, i)
		}
	}
}

func (kv *RaftKV) listenToStateChanges() {
	exit := false
	for {
		select {
		case <-kv.stopListenToStateChanges:
			exit = true
		default:
			kv.handleStateChange()
			time.Sleep(time.Millisecond * 250)
		}

		if exit {
			break
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {

	reply.WrongLeader = false
	reply.Err = ""

	kv.mu.Lock()

	op := Op{"Get", args.Key, "", args.CommandID}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	replyCh := make(chan rpcReply)
	kv.waitingOps[args.CommandID] = replyCh
	// fmt.Printf("s%dwaiting for completion eI:%d eT:%d args:%v reply:%v\n", kv.me, eI, eT, args, reply)

	kv.mu.Unlock()

	// wait for msg to be applied
	resultFromCh := <-replyCh
	reply.Err = resultFromCh.Err
	reply.Value = resultFromCh.result
	reply.WrongLeader = false
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	reply.WrongLeader = false
	reply.Err = ""

	kv.mu.Lock()

	op := Op{args.Op, args.Key, args.Value, args.CommandID}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	replyCh := make(chan rpcReply)
	kv.waitingOps[args.CommandID] = replyCh
	// fmt.Printf("s%dwaiting for completion eI:%d eT:%d args:%v reply:%v\n", kv.me, eI, eT, args, reply)

	kv.mu.Unlock()

	// wait for msg to be applied
	resultFromCh := <-replyCh
	reply.Err = resultFromCh.Err
	reply.WrongLeader = false
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()

	kv.stopListenToApplyCh <- true
	kv.stopListenToStateChanges <- true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	// fmt.Println("# of servers: ", len(servers))

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.values = make(map[string]string)
	kv.isLeader = false
	kv.currentTerm = 0

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stopListenToApplyCh = make(chan bool)
	kv.stopListenToStateChanges = make(chan bool)
	kv.opHistory = make(map[int64]OpRecord)
	kv.waitingOps = make(map[int64]chan rpcReply)

	go kv.listenToApplyCh()
	go kv.listenToStateChanges()

	return kv
}
