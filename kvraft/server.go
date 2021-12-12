package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
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
	commandID       int64
	replyCh         chan rpcReply
	expectedIndex   int
	expectedTerm    int
	isExecuted      bool
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

	isLeader bool

	// Your definitions here.
	values map[string]string

	// recordOfPendingIntents
	opRecords map[int64]OpRecord

	// kill
	stopListenToApplyCh chan bool
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
		fmt.Println("WHATTT")
	}
	return result
}

func (kv *RaftKV) modifyOpRecord(commandID int64, result string) chan rpcReply {
	prevRec := kv.opRecords[commandID]
	kv.opRecords[commandID] = OpRecord{
		commandID:       prevRec.commandID,
		replyCh:         prevRec.replyCh,
		expectedIndex:   prevRec.expectedIndex,
		expectedTerm:    prevRec.expectedTerm,
		isExecuted:      true,
		executionResult: result,
	}
	return prevRec.replyCh
}

func (kv *RaftKV) notifyToRPCCh(replyCh chan rpcReply, result string) {
	replyCh <- rpcReply{result, ""}
}

func (kv *RaftKV) handleApply(log raft.ApplyMsg) {
	kv.mu.Lock()

	// execute
	op := log.Command.(Op)
	result := kv.executeOperation(op)
	kv.printValues(op)

	wasLeader := kv.isLeader
	_, isLeader := kv.rf.GetState()
	kv.isLeader = isLeader

	if isLeader {
		// modify op record
		replyCh := kv.modifyOpRecord(op.CommandID, result)
		me, name, k, v, cmdID := kv.me, op.OpName, op.Key, op.Value, op.CommandID
		kv.mu.Unlock()
		// send to appropriate channel
		fmt.Printf("s%d notifToCh[*]: %s, k:%s, v:%s, %v\n", me, name, k, v, cmdID)
		kv.notifyToRPCCh(replyCh, result)
		fmt.Printf("s%d notifToCh[**]: %s, k:%s, v:%s, %v\n", me, name, k, v, cmdID)
	} else {
		// if it considered itself leader previously
		if wasLeader {
			// clear state
			for k, v := range kv.opRecords {
				if !v.isExecuted {
					fmt.Println("!")
					v.replyCh <- rpcReply{"", "leader changed!"}
					fmt.Println("!!")
				}
				delete(kv.opRecords, k)
			}
		}
		kv.mu.Unlock()
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

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {

	reply.WrongLeader = false
	reply.Err = ""

	kv.mu.Lock()

	opRecord, opRecordExists := kv.opRecords[args.CommandID]
	var expectedIndex, expectedTerm int

	if opRecordExists {
		if opRecord.isExecuted {
			reply.Value = opRecord.executionResult
			fmt.Printf("s%d isExecuted...replying args:%v reply:%v\n", kv.me, args, reply)
			kv.mu.Unlock()
			return
		} else {
			// wait for it to be executed
			expectedIndex = opRecord.expectedIndex
			expectedTerm = opRecord.expectedTerm
			fmt.Printf("s%d exists...eI:%d eT:%d args:%v reply:%v\n", kv.me, expectedIndex, expectedTerm, args, reply)
		}
	} else {
		var isLeader bool
		expectedIndex, expectedTerm, isLeader = kv.rf.Start(Op{"Get", args.Key, "", args.CommandID})
		fmt.Printf("s%d started...iL:%v eI:%d eT:%d args:%v reply:%v\n", kv.me, isLeader, expectedIndex, expectedTerm, args, reply)
		if !isLeader {
			reply.WrongLeader = true
			fmt.Printf("s%d not leader...replying eI:%d eT:%d args:%v reply:%v\n", kv.me, expectedIndex, expectedTerm, args, reply)
			kv.mu.Unlock()
			return
		}
	}

	replyCh := make(chan rpcReply)

	kv.opRecords[args.CommandID] = OpRecord{
		commandID:       args.CommandID,
		replyCh:         replyCh,
		expectedIndex:   expectedIndex,
		expectedTerm:    expectedTerm,
		isExecuted:      false,
		executionResult: "",
	}

	fmt.Printf("s%dwaiting for completion eI:%d eT:%d args:%v reply:%v\n", kv.me, expectedIndex, expectedTerm, args, reply)

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

	opRecord, opRecordExists := kv.opRecords[args.CommandID]
	var expectedIndex, expectedTerm int

	if opRecordExists {
		if opRecord.isExecuted {
			fmt.Printf("s%d isExecuted...replying args:%v reply:%v\n", kv.me, args, reply)
			kv.mu.Unlock()
			return
		} else {
			// wait for it to be executed
			expectedIndex = opRecord.expectedIndex
			expectedTerm = opRecord.expectedTerm
			fmt.Printf("s%d exists...eI:%d eT:%d args:%v reply:%v\n", kv.me, expectedIndex, expectedTerm, args, reply)
		}
	} else {
		var isLeader bool
		expectedIndex, expectedTerm, isLeader = kv.rf.Start(Op{args.Op, args.Key, args.Value, args.CommandID})
		fmt.Printf("s%d started...iL:%v eI:%d eT:%d args:%v reply:%v\n", kv.me, isLeader, expectedIndex, expectedTerm, args, reply)
		if !isLeader {
			reply.WrongLeader = true
			fmt.Printf("s%d not leader...replying eI:%d eT:%d args:%v reply:%v\n", kv.me, expectedIndex, expectedTerm, args, reply)
			kv.mu.Unlock()
			return
		}
	}

	replyCh := make(chan rpcReply)

	kv.opRecords[args.CommandID] = OpRecord{
		commandID:       args.CommandID,
		replyCh:         replyCh,
		expectedIndex:   expectedIndex,
		expectedTerm:    expectedTerm,
		isExecuted:      false,
		executionResult: "",
	}

	fmt.Printf("s%dwaiting for completion eI:%d eT:%d args:%v reply:%v\n", kv.me, expectedIndex, expectedTerm, args, reply)

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
	// Your code here, if desired.
	kv.stopListenToApplyCh <- true
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

	fmt.Println("# of servers: ", len(servers))

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.values = make(map[string]string)
	kv.isLeader = false

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stopListenToApplyCh = make(chan bool)
	kv.opRecords = make(map[int64]OpRecord)

	go kv.listenToApplyCh()

	return kv
}
