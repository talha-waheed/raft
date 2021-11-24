// package raft

// import (
// 	"fmt"
// 	"labrpc"
// 	"math/rand"
// 	"sync"
// 	"time"
// )

// type ApplyMsg struct {
// 	Index       int
// 	Command     interface{}
// 	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
// 	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
// }

// type LogStruct struct {
// 	Term    int
// 	Command interface{}
// }

// //
// // A Go object implementing a single Raft peer.
// //
// type Raft struct {
// 	mu          sync.Mutex
// 	peers       []*labrpc.ClientEnd
// 	log         []LogStruct
// 	persister   *Persister
// 	me          int
// 	currentTerm int
// 	votedFor    int
// 	timerFlag   bool
// 	isLeader    bool
// 	isKilled    bool
// 	commitCh    chan int

// 	commitIndex int
// 	lastApplied int

// 	nextIndex  []int
// 	matchIndex []int
// }

// // return currentTerm and whether this server
// // believes it is the leader.
// func (rf *Raft) GetState() (int, bool) {
// 	var term int
// 	var isleader bool
// 	rf.mu.Lock()
// 	term = rf.currentTerm
// 	isleader = rf.isLeader
// 	rf.mu.Unlock()
// 	return term, isleader
// }

// func (rf *Raft) persist() {

// }

// //
// // restore previously persisted state.
// //
// func (rf *Raft) readPersist(data []byte) {
// 	// Your code here.
// 	// Example:
// 	// r := bytes.NewBuffer(data)
// 	// d := gob.NewDecoder(r)
// 	// d.Decode(&rf.xxx)
// 	// d.Decode(&rf.yyy)
// }

// type RequestVoteArgs struct {
// 	Term         int
// 	CandidateID  int
// 	LastLogIndex int
// 	LastLogTerm  int
// }

// type RequestVoteReply struct {
// 	Term        int
// 	VoteGranted bool
// }

// type AsyncRequestVoteReply struct {
// 	Ok    bool
// 	Reply RequestVoteReply
// }

// type AppendEntriesArg struct {
// 	Term         int
// 	LeaderID     int
// 	PrevLogIndex int
// 	PrevLogTerm  int
// 	Entries      []LogStruct
// 	LeaderCommit int
// }

// type AppendEntriesReply struct {
// 	Term    int
// 	Success bool
// }

// type AsyncAppendEntriesReply struct {
// 	Ok    bool
// 	Id    int
// 	Reply AppendEntriesReply
// }

// // RequestVote RPC implementation
// func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
// 	rf.mu.Lock()
// 	rf.timerFlag = false
// 	cT := rf.currentTerm
// 	vF := rf.votedFor
// 	myLastTerm := -1
// 	myLastIndex := -1
// 	if len(rf.log) > 0 {
// 		myLastTerm = rf.log[len(rf.log)-1].Term //try except
// 		myLastIndex = len(rf.log) - 1
// 	}
// 	rf.mu.Unlock()
// 	reply.Term = cT
// 	if (myLastTerm > args.LastLogTerm) || ((myLastTerm == args.LastLogTerm) && (myLastIndex > args.LastLogIndex)) {
// 		reply.VoteGranted = false
// 	} else if args.Term > cT { // grant vote if candidate's term is greater than mine and check safety condition
// 		reply.VoteGranted = true
// 		rf.mu.Lock()
// 		rf.votedFor = args.CandidateID
// 		rf.currentTerm = args.Term
// 		rf.mu.Unlock()
// 	} else if (args.Term == cT) && (vF == -1 || vF == args.CandidateID) { // grant vote if candidate's term is equal to mine and I have either not voted for anyone or have voted for this candidate before
// 		reply.VoteGranted = true
// 		rf.mu.Lock()
// 		rf.votedFor = args.CandidateID
// 		rf.mu.Unlock()
// 	} else {
// 		reply.VoteGranted = false
// 	}
// }

// // AppendEntries RPC implementation
// func (rf *Raft) AppendEntries(args AppendEntriesArg, reply *AppendEntriesReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	cT := rf.currentTerm
// 	var lastLogIndex int
// 	var lastLogTerm int

// 	reply.Term = cT
// 	if args.Term < cT { // return success as false if leader's term is greater than mine
// 		reply.Success = false
// 		return
// 	} else {
// 		rf.timerFlag = false
// 		rf.currentTerm = args.Term
// 		rf.isLeader = false
// 	}

// 	if args.PrevLogIndex == -1 { // when append entries are without logs and meant to serve as heartbeat only
// 		if len(args.Entries) != 0 {
// 			rf.log = append(rf.log, args.Entries...)
// 		}
// 		reply.Success = true
// 	} else {
// 		// set lastLogTerm and lastLogIndex of a follower
// 		// fmt.Println(rf.log)
// 		lastLogIndex = len(rf.log) - 1
// 		if lastLogIndex < args.PrevLogIndex {
// 			lastLogTerm = -2
// 		} else {
// 			lastLogTerm = rf.log[args.PrevLogIndex].Term
// 		}

// 		// check Log Matching Property and accordingly, update the success bool
// 		if lastLogTerm == args.PrevLogTerm && args.PrevLogIndex == lastLogIndex {
// 			rf.log = trimSliceAtIndex(rf.log, args.PrevLogIndex+1) // delete index val and all that follows it
// 			rf.log = append(rf.log, args.Entries...)
// 			if len(args.Entries) > 0 {
// 				fmt.Println(rf.me, ":", rf.commitIndex, rf.lastApplied, rf.log, "(log added through appendEntries)")
// 			}
// 			reply.Success = true
// 		} else {
// 			reply.Success = false
// 			// fmt.Println("lastlogTerm: ", lastLogTerm, " prevLogTerm: ", args.PrevLogTerm, " prevlogIndex: ", args.PrevLogIndex, " lastLogIndex: ", lastLogIndex)
// 		}

// 		// fmt.Println("me: ", rf.me, " my c index: ", rf.commitIndex, "leader c index: ", rf.commitIndex)
// 		// fmt.Println(args.Entries, " prevlogindex: ", args.PrevLogIndex, " prevlogterm: ", args.PrevLogTerm, " log: ", rf.log)

// 		// set commit index
// 		if args.LeaderCommit > rf.commitIndex && reply.Success {
// 			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
// 			fmt.Println(rf.me, ":", rf.commitIndex, rf.lastApplied, rf.log, "(log applied)")
// 			if rf.commitIndex > rf.lastApplied {
// 				rf.commitCh <- rf.commitIndex
// 			}
// 		}

// 	}
// 	// reply.Term = cT
// 	// if args.Term < cT { // return success as false if leader's term is greater than mine
// 	// 	reply.Success = false
// 	// } else {
// 	// 	rf.mu.Lock()
// 	// 	rf.currentTerm = args.Term
// 	// 	rf.isLeader = false
// 	// 	rf.mu.Unlock()
// 	// }

// }

// func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArg, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	index := -1
// 	term := -1
// 	rf.mu.Lock()
// 	isLeader := rf.isLeader
// 	if isLeader && (rf.log[len(rf.log)-1].Command != command) {
// 		// fmt.Println("me: ", rf.me, "command: ", command)
// 		rf.log = append(rf.log, LogStruct{rf.currentTerm, command})
// 		index = len(rf.log) - 1
// 		fmt.Println(rf.me, ":", rf.commitIndex, rf.lastApplied, rf.log, "(added log to leader)")
// 	}
// 	rf.mu.Unlock()

// 	return index, term, isLeader
// }

// func (rf *Raft) Kill() {
// 	rf.mu.Lock()
// 	rf.isKilled = true
// 	rf.mu.Unlock()
// }

// // Creating a raft server
// func Make(peers []*labrpc.ClientEnd, me int,
// 	persister *Persister, applyCh chan ApplyMsg) *Raft {
// 	rf := &Raft{}
// 	rf.peers = peers
// 	rf.persister = persister
// 	rf.me = me
// 	rf.currentTerm = 0
// 	rf.isLeader = false
// 	rf.isKilled = false
// 	rf.commitIndex = 0
// 	rf.lastApplied = 0
// 	rf.votedFor = -1
// 	rf.log = append(rf.log, LogStruct{rf.currentTerm, -1})
// 	rf.commitCh = make(chan int)
// 	go rf.stateMachine(applyCh)
// 	rf.commitCh <- 0
// 	go rf.stateHandler(applyCh)
// 	rf.readPersist(persister.ReadRaftState())

// 	return rf
// }

// // this function starts a go routine for tracing the election timeouts which runs in the background while it starts a for loop that sets the case to either a candidate status or a Leader status and calls their respective go routine i.e send requestVotes or send appendEntries
// func (rf *Raft) stateHandler(applyCh chan ApplyMsg) {
// 	var makeCandidate = make(chan bool)
// 	var makeLeader = make(chan bool)
// 	var resetTimerFlag = make(chan bool)

// 	go rf.electionTimeout(makeCandidate, resetTimerFlag)
// 	for {
// 		select {
// 		case <-makeCandidate:
// 			go rf.sendRequestVoteToAll(makeLeader, resetTimerFlag)
// 		case <-makeLeader:
// 			rf.mu.Lock()
// 			rf.nextIndex = make([]int, len(rf.peers))
// 			rf.matchIndex = make([]int, len(rf.peers))
// 			for i := range rf.nextIndex {
// 				rf.nextIndex[i] = len(rf.log)
// 			}
// 			rf.mu.Unlock()
// 			go rf.appendEntries()

// 		}

// 	}
// }

// // this function keeps track of election timeout. It runs forever in the background as long as the raft is alive. In case the raft does not hear from the leader or election fails, it assumes candidate position as soon as the random timeout is reached
// func (rf *Raft) electionTimeout(makeCandidate chan bool, resetTimerFlag chan bool) {
// 	for {
// 		rf.mu.Lock()
// 		rf.timerFlag = true
// 		rf.mu.Unlock()
// 		rnd := rand.Intn(300) + 600 //rand time b/w 300,600ms
// 		time.Sleep(time.Millisecond * time.Duration(rnd))
// 		rf.mu.Lock()
// 		flag := rf.timerFlag
// 		isLeader := rf.isLeader
// 		if rf.isKilled {
// 			rf.mu.Unlock()
// 			break
// 		}
// 		rf.mu.Unlock()
// 		if flag && !isLeader {
// 			makeCandidate <- true // initiates candidate phase
// 			rf.mu.Lock()
// 			fmt.Println(rf.me, "is now a candidate")
// 			rf.mu.Unlock()
// 			<-resetTimerFlag
// 		}
// 	}
// }

// // function to asynchronously grab votes from servers through RPC
// func (rf *Raft) sendAsyncRequestVote(chanAsyncReply chan AsyncRequestVoteReply, serverID int, args RequestVoteArgs, reply *RequestVoteReply) {
// 	ok := rf.sendRequestVote(serverID, args, reply) //RPC for requestVote
// 	chanAsyncReply <- AsyncRequestVoteReply{ok, RequestVoteReply{reply.Term, reply.VoteGranted}}
// }

// // this function sends vote requests to all rafts and allows the local raft to assume leader position according to the votes gained
// func (rf *Raft) sendRequestVoteToAll(makeLeader chan bool, resetTimerFlag chan bool) {
// 	rf.mu.Lock()
// 	rf.currentTerm += 1
// 	rf.votedFor = rf.me
// 	peersCount := len(rf.peers)
// 	args := RequestVoteArgs{rf.currentTerm, rf.me, -1, -1}
// 	if len(rf.log) > 0 {
// 		args = RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
// 	}

// 	rf.mu.Unlock()
// 	resetTimerFlag <- true

// 	count := 1
// 	toFollower := false
// 	chanAsyncReply := make(chan AsyncRequestVoteReply, peersCount-1) // channel used to grab RPC responses

// 	// Run RequestVote RPC for all servers asynchronously
// 	for x := 0; x < peersCount; x++ {
// 		if x != args.CandidateID {
// 			go rf.sendAsyncRequestVote(chanAsyncReply, x, args, &RequestVoteReply{})
// 		}
// 	}

// 	msgCounts := 1
// 	for {
// 		asyncReply := <-chanAsyncReply // getting responses from channel
// 		msgCounts++
// 		ok := asyncReply.Ok
// 		reply := asyncReply.Reply
// 		if ok {
// 			rf.mu.Lock()
// 			if reply.Term > rf.currentTerm || rf.votedFor != rf.me {
// 				rf.currentTerm = reply.Term
// 				rf.mu.Unlock()
// 				toFollower = true
// 				break
// 			}
// 			rf.mu.Unlock()
// 			if reply.VoteGranted {
// 				count += 1
// 				if !toFollower && count > peersCount/2 {
// 					rf.mu.Lock()
// 					rf.timerFlag = false
// 					rf.isLeader = true
// 					fmt.Println("leader is now", rf.me)
// 					rf.mu.Unlock()
// 					makeLeader <- true
// 					break
// 				}
// 			}
// 		}
// 		// checking if all responses have been received
// 		if msgCounts == peersCount {
// 			break
// 		}
// 	}
// }

// func (rf *Raft) sendAsyncAppendEntries(chanAsyncReply chan AsyncAppendEntriesReply, serverID int, args AppendEntriesArg, reply *AppendEntriesReply) {
// 	ok := rf.sendAppendEntries(serverID, args, reply)
// 	chanAsyncReply <- AsyncAppendEntriesReply{ok, serverID, AppendEntriesReply{reply.Term, reply.Success}}
// }

// // This function runs when a leader position is take by a server. It keeps sending heartbeats to all other servers. This runs until the leader receives a term greater than its current term or it receive append entry from a leader with equal or higher term
// func (rf *Raft) appendEntries() {

// 	rf.mu.Lock()
// 	peersCount := len(rf.peers)
// 	rf.mu.Unlock()

// 	chanAsyncReply := make(chan AsyncAppendEntriesReply, peersCount)

// 	for {

// 		logLen := 0

// 		// Run AppendEntries RPC for all servers asynchronously
// 		for x := 0; x < peersCount; x++ {

// 			rf.mu.Lock()
// 			prevLogTerm := rf.log[rf.nextIndex[x]-1].Term
// 			prevLogIndex := rf.nextIndex[x] - 1

// 			args := AppendEntriesArg{rf.currentTerm, rf.me, -1, -1, []LogStruct{}, rf.commitIndex}
// 			// if len(rf.log) >= rf.nextIndex[x] {
// 			args = AppendEntriesArg{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.log[rf.nextIndex[x]:], rf.commitIndex}
// 			// }

// 			logLen = len(rf.log)

// 			rf.mu.Unlock()
// 			if x != args.LeaderID {
// 				go rf.sendAsyncAppendEntries(chanAsyncReply, x, args, &AppendEntriesReply{})
// 			}
// 		}

// 		// count := 1
// 		msgCounts := 1
// 		for {
// 			asyncReply := <-chanAsyncReply // getting responses from channel
// 			msgCounts++
// 			ok := asyncReply.Ok
// 			reply := asyncReply.Reply
// 			serverID := asyncReply.Id
// 			if ok {
// 				rf.mu.Lock()
// 				if reply.Term > rf.currentTerm || !rf.isLeader {
// 					rf.currentTerm = reply.Term
// 					rf.isLeader = false
// 					rf.mu.Unlock()
// 					return
// 				}

// 				// New Code

// 				if reply.Success {

// 					// next index theek karo
// 					rf.nextIndex[serverID] = logLen

// 					// match index
// 					rf.matchIndex[serverID] = rf.nextIndex[serverID] - 1

// 					// update commit
// 					commitUpdated := false
// 					// if rf.commitIndex < logLen-1 {
// 					// fmt.Println(rf.matchIndex, rf.nextIndex)
// 					for N := rf.commitIndex + 1; N < logLen; N++ {
// 						count := 1
// 						for i, _ := range rf.peers {
// 							if rf.matchIndex[i] >= N {
// 								count++
// 							}
// 						}
// 						if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
// 							rf.commitIndex = N
// 							commitUpdated = true
// 						}
// 					}
// 					// }
// 					rf.mu.Unlock()

// 					// apply to state machine
// 					if commitUpdated {
// 						rf.commitCh <- rf.commitIndex
// 					}

// 				} else {
// 					rf.mu.Lock()
// 					rf.nextIndex[serverID] -= 1
// 					rf.mu.Unlock()
// 				}

// 				// end of new code

// 				// if len(rf.log) >= rf.nextIndex[serverID] {
// 				// 	if reply.Success {
// 				// 		rf.nextIndex[serverID] = len(rf.log) //initilized to leader last log index + 1
// 				// 		count += 1
// 				// 		if count > peersCount/2 && (len(rf.log)-1 > rf.commitIndex) {
// 				// 			rf.commitIndex += 1
// 				// 			rf.commitCh <- rf.commitIndex
// 				// 			fmt.Println(rf.me, ":", rf.commitIndex, rf.lastApplied, rf.log, "(log committed)")
// 				// 		}
// 				// 	} else {
// 				// 		rf.nextIndex[serverID] -= 1
// 				// 	}
// 				// }

// 			}
// 			if msgCounts == peersCount {
// 				break
// 			}
// 		}

// 		time.Sleep(time.Second / 10) // 10 heartbeats per sec
// 	}
// }

// func (rf *Raft) stateMachine(applyCh chan ApplyMsg) {
// 	for {
// 		commitIndex := <-rf.commitCh

// 		// rnd := rand.Intn(300) + 600

// 		// fmt.Println("came here", rnd)

// 		rf.mu.Lock()
// 		lastApplied := rf.lastApplied
// 		entriesToApply := rf.log[rf.lastApplied : commitIndex+1]
// 		entriesToApplyCopied := make([]LogStruct, len(entriesToApply))
// 		copy(entriesToApplyCopied, entriesToApply)
// 		rf.mu.Unlock()

// 		for i, val := range entriesToApplyCopied {
// 			applyCh <- ApplyMsg{lastApplied + i, val.Command, false, nil}
// 		}

// 		rf.mu.Lock()
// 		rf.lastApplied = commitIndex
// 		if rf.isKilled {
// 			break
// 		}

// 		fmt.Println(rf.me, ":", rf.commitIndex, rf.lastApplied, rf.log, "(log applied)")
// 		rf.mu.Unlock()

// 		// fmt.Println("got out", rnd)
// 	}
// }

// func trimSliceAtIndex(arr []LogStruct, i int) []LogStruct {
// 	var newArr []LogStruct
// 	for j := 0; j < i; j++ {
// 		newArr = append(newArr, arr[j])
// 	}
// 	return newArr
// }

// func min(a, b int) int {
// 	if a < b {
// 		return a
// 	}
// 	return b
// }
