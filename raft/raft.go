package raft

// 1. persists lagao
// 2. use locks more conservatively
// 3. independently chalao RPCs in go routine(s)
// 4. run getRPC in RPC itself

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

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// heartbeat and election timeout in ms
const MinTimeout = 300
const MaxTimeout = 600

// heartbeat interval
const HeartbeatInterval = time.Second / 10

// interval to periodically check for committable entries
const CheckCommittableInterval = time.Second / 10

// Null value for vote granted
const Null int = -1

// Struct for a log entry
type Log struct {
	Index   int
	Term    int
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// persistent states
	currentTerm      int
	votedFor         int
	log              []Log
	currentRaftState string // specific to this impl. of raft

	// volatile states
	commitIndex int
	lastApplied int

	// leader volatile state
	nextIndex  []int
	matchIndex []int

	// channels to recieve information from RPCs
	appendEntriesArgsCh     chan AppendEntriesArgs
	appendEntriesReplyCh    chan AppendEntriesReply
	requestVoteArgsCh       chan RequestVoteArgs
	requestVoteReplyCh      chan RequestVoteReply
	notifyNewCmdCh          chan interface{}
	kickStartApplyEntriesCh chan int
	applyCh                 chan ApplyMsg

	// chans to end raft
	endApplyToStateMachineCh chan bool
	endRaft                  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentRaftState == "leader"
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.currentRaftState)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.currentRaftState)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// send the rpc request over to the follower/candidate/leader go routine running rn
	rf.requestVoteArgsCh <- args

	// it will examine server states and reply with the reply
	replyFromChan := <-rf.requestVoteReplyCh

	// send the reply received back
	reply.Term = replyFromChan.Term
	reply.VoteGranted = replyFromChan.VoteGranted
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	// send the rpc request over to the follower/candidate/leader go routine running rn
	rf.appendEntriesArgsCh <- args

	// it will examine server states and reply with the reply
	replyFromChan := <-rf.appendEntriesReplyCh

	// send the reply received back
	reply.Success = replyFromChan.Success
	reply.Term = replyFromChan.Term
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func printMyID(me int, currentRaftState string) string {
	if currentRaftState == "leader" {
		return fmt.Sprintf("[%d]", me)
	}
	return fmt.Sprintf("%d", me)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.currentRaftState == "leader"
	if isLeader {
		rf.log = append(rf.log, Log{index, term, command})
		// fmt.Println("s:", printMyID(rf.me, rf.currentRaftState), ": [add to log at leader] t:", rf.currentTerm, "cI:", rf.commitIndex, "lA:", rf.lastApplied, rf.log, "(added to log at leader)")
		rf.persist()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.endApplyToStateMachineCh <- true
	rf.endRaft <- true
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
	rf.applyCh = applyCh

	// initialize our state variables
	rf.initializeServerVars(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start the server in the follower state (this doesn't block)
	rf.startServerState()

	// start a routine that can apply logs to state machine
	go rf.applyToStateMachine()

	return rf
}

func (rf *Raft) initializeServerVars(applyCh chan ApplyMsg) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// persistent state
	rf.currentTerm = 0
	rf.votedFor = Null
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{
		Index: 0,
		Term:  Null,
	})
	rf.currentRaftState = "follower"

	// volatile state
	rf.commitIndex = 0
	rf.lastApplied = 0

	// note: leader volatile state will be initialized when server becomes leader

	// chans for communication
	rf.appendEntriesArgsCh = make(chan AppendEntriesArgs)
	rf.appendEntriesReplyCh = make(chan AppendEntriesReply)
	rf.requestVoteArgsCh = make(chan RequestVoteArgs)
	rf.requestVoteReplyCh = make(chan RequestVoteReply)
	rf.notifyNewCmdCh = make(chan interface{})
	rf.kickStartApplyEntriesCh = make(chan int)

	// chans to end go routines
	rf.endApplyToStateMachineCh = make(chan bool)
	rf.endRaft = make(chan bool)
}

func printWarning(toPrint string) {
	// // fmt.Println("Warning: " + toPrint)
}

func (rf *Raft) startServerState() {
	// rf.mu.Lock()
	// currentState := rf.currentRaftState
	// rf.mu.Unlock()

	// rf.changeServerStateTo(currentState)
	rf.changeServerStateTo("follower")
}

// changes Raft's server state
func (rf *Raft) changeServerStateTo(newServerState string) {

	rf.mu.Lock()
	// fmt.Println("s:", printMyID(rf.me, rf.currentRaftState), fmt.Sprintf(": st.c. %s->%s] t:", rf.currentRaftState[0:1], newServerState[0:1]), rf.currentTerm, "cI:", rf.commitIndex, "lA:", rf.lastApplied, rf.log, "(added to log at leader)")
	rf.currentRaftState = newServerState
	rf.persist()
	rf.mu.Unlock()

	if newServerState == "follower" {
		go rf.beFollower()
	} else if newServerState == "candidate" {
		go rf.beCandidate()
	} else if newServerState == "leader" {
		go rf.beLeader()
	} else {
		printWarning(fmt.Sprintf("%s is an invalid state", newServerState))
	}
}

// get Random Timeout Interval for Election Timeouts
func getRandomTimeoutInterval() time.Duration {
	return time.Duration(rand.Intn(MaxTimeout-MinTimeout)+MinTimeout) * time.Millisecond
}

// kickstarts code to apply unapplied but committed entries
func (rf *Raft) applyNewCommitsToStateMachine() {
	rf.kickStartApplyEntriesCh <- 0
}

// called by follower, candidate and leader to reply to an AppendEntry RPC
func (rf *Raft) getAppendEntriesReply(args AppendEntriesArgs) (AppendEntriesReply, bool) {

	// wrap the execution of this func in locks
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := AppendEntriesReply{}
	isHeartbeatValid := false

	if args.Term < rf.currentTerm {

		// current term gt sender's term, reject this msg
		reply.Success = false
		// and tell leader to update its term to rf.currentTerm
		reply.Term = rf.currentTerm

	} else {

		// if this is the same or greater term, then this is the new leader, iski bait karo
		// inform leader that we agree to it being the leader
		isHeartbeatValid = true
		reply.Success = true
		reply.Term = args.Term

		// change our term to conform to the leader
		rf.currentTerm = args.Term
		// if we had vote for anyone, clear it. We are beyond candidate state
		rf.votedFor = Null

		// ensure prev log is same, else reject it
		if !rf.isPrevLogSame(args.PrevLogIndex, args.PrevLogTerm) {

			reply.Success = false
			// // fmt.Println(rf.me, ": t:", rf.currentTerm, "cI:", rf.commitIndex, "lA:", rf.lastApplied, rf.log, "(prev not same: rejected heartbeat from leader)")

		} else {

			// add the new entries to local log
			rf.addEntriesFromLeaderToLog(args)
			// if there is committable entries, commit them and pass them to state machine
			if args.LeaderCommit > rf.commitIndex {
				rf.commitNewEntries(args.LeaderCommit)
				if rf.lastApplied < rf.commitIndex {
					go rf.applyNewCommitsToStateMachine()
				}
			}

			reply.Success = true
			// // fmt.Println("added log from leader in", rf.me, "commitIndex:", rf.commitIndex, "log:", rf.log)
		}

		rf.persist()
	}

	// the calling function (beFollower, beCandidate, or beLeader) will alter its behavior by inspecting this reply
	// e.g. the candidate will fall back to become a follower if reply.Success == true
	return reply, isHeartbeatValid
}

func (rf *Raft) commitNewEntries(leaderCommit int) {
	lastLogIndex := len(rf.log) - 1
	rf.commitIndex = min(leaderCommit, lastLogIndex)
}

func (rf *Raft) addEntriesFromLeaderToLog(args AppendEntriesArgs) {

	// prune log until previndex
	prunedLogSlice := rf.log[:args.PrevLogIndex+1]
	rf.log = prunedLogSlice

	// add entries ahead of it
	rf.log = append(prunedLogSlice, args.Entries...)

	// fmt.Println("s:", printMyID(rf.me, rf.currentRaftState), fmt.Sprintf(": [add to log from hb of %d] t:", args.LeaderId), rf.currentTerm, "cI:", rf.commitIndex, "lA:", rf.lastApplied, rf.log, "(added", args.Entries, "from leader in heartbeat)")
}

func (rf *Raft) isPrevLogSame(prevLogIndex int, prevLogTerm int) bool {

	// // if there was no prevlog at the leader, return true
	// if prevLogIndex == 0 {
	// 	return true
	// }

	// check if there is some entry at index == prevLogIndex
	if len(rf.log) < prevLogIndex+1 {
		return false
	}

	// check if the term at that index is the same
	if rf.log[prevLogIndex].Term == prevLogTerm {
		return true
	} else {
		return false
	}
}

// called by follower, candidate and leader to reply to an RequestVote RPC
func (rf *Raft) getRequestVoteReply(args RequestVoteArgs) RequestVoteReply {

	// wrap the execution of this func in locks
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := RequestVoteReply{}

	if rf.currentTerm > args.Term {
		// if our term is greater, reject the vote and send the updated term for the server to update itself
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else {
		// if our term is less or equal to candidate's term, grant vote if log is not more complete

		haveNotVotedBefore := rf.votedFor == Null || rf.votedFor == args.CandidateId

		lastLogV := rf.log[len(rf.log)-1]
		isOurLogMoreComplete := (lastLogV.Term > args.LastLogTerm) || ((lastLogV.Term == args.LastLogTerm) && (lastLogV.Index > args.LastLogIndex))

		if (rf.currentTerm < args.Term || haveNotVotedBefore) && !isOurLogMoreComplete {
			reply.Term = args.Term
			reply.VoteGranted = true

			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId

			// // fmt.Println(rf.me, ": t:", rf.currentTerm, "cI:", rf.commitIndex, "lA:", rf.lastApplied, "(RV: cT changed from", rf.currentTerm, "to", args.Term, ")")

		} else {
			reply.Term = args.Term
			reply.VoteGranted = false

			rf.currentTerm = args.Term
			rf.votedFor = Null
		}

		rf.persist()
	}

	// We have updated state variables in this function (rf.currentTerm and rf.votedFor).
	// However, the calling function (beFollower, beCandidate, or beLeader) will be responsible
	// to alter its behavior by inspecting this reply. e.g. the candidate will fall back to
	// become a follower if reply.Success == true
	return reply
}

func (rf *Raft) beFollower() {

	// reset voted for before becoming a follower
	rf.mu.Lock()
	myID := rf.me
	rf.persist()
	rf.mu.Unlock()

	// bool to break state's infinite for loop
	exitFollowerState := false

	// get timer for a heartbeat interval
	heartbeatTimer := time.NewTimer(getRandomTimeoutInterval())
	t1 := time.Now()

	for {

		select {

		// if raft server has been killed
		case <-rf.endRaft:
			exitFollowerState = true

		// if heartbeat times out
		case <-heartbeatTimer.C:
			// STATE TRANSITION: follower -> candidate
			printWarning(fmt.Sprintf("hearbeat timeout %d with elapsed: %d", myID, time.Since(t1)/1000000))
			exitFollowerState = true            // exit the follower state
			rf.changeServerStateTo("candidate") // become a candidate

		// if we get a heartbeat
		case args := <-rf.appendEntriesArgsCh:

			// reply to append entries
			reply, isHeartbeatValid := rf.getAppendEntriesReply(args)
			rf.appendEntriesReplyCh <- AppendEntriesReply{reply.Term, reply.Success}

			// as a follower, there is nothing to do with the reply, except to restart timer

			// if legitimate heartbeat
			if isHeartbeatValid {
				// restart timer (as we received a heartbeat!):
				// 1. stop timer
				if !heartbeatTimer.Stop() {
					// just ensuring that if the channel has value,
					// drain it before restarting (so we dont leak resources
					// for keeping the channel indefinately up)
					<-heartbeatTimer.C
				}
				// 2. reset timer
				// printWarning(fmt.Sprintf("resetting follower %d's heartbeat", myID))
				heartbeatTimer.Reset(getRandomTimeoutInterval())
				t1 = time.Now()
			}

		// reply to requestVoteRPCs
		case args := <-rf.requestVoteArgsCh:

			reply := rf.getRequestVoteReply(args)
			rf.requestVoteReplyCh <- RequestVoteReply{reply.Term, reply.VoteGranted}

			// if we granted the vote
			if reply.VoteGranted {
				// restart timer (as we received a reqvote):
				// 1. stop timer
				if !heartbeatTimer.Stop() {
					// just ensuring that if the channel has value,
					// drain it before restarting (so we dont leak resources
					// for keeping the channel indefinately up)
					<-heartbeatTimer.C
				}
				// 2. reset timer
				heartbeatTimer.Reset(getRandomTimeoutInterval())
				t1 = time.Now()
			}
		}

		if exitFollowerState {
			break
		}

	}
}

// wrapper func around sendRequestVote(...) to receive RPC reply in a channel
func (rf *Raft) sendRequestVoteRPC(chanReqVoteReplies chan RequestVoteReply, server int, args RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(server, args, reply)
	if ok {
		chanReqVoteReplies <- RequestVoteReply{reply.Term, reply.VoteGranted}
	}
}

func (rf *Raft) callReqVoteRPCs(chanHasWonElection chan bool, chanUpdatedTerm chan int, chanCancelElection chan bool) {

	// makePayLoad
	rf.mu.Lock()
	numOfPeers := len(rf.peers)
	myTerm := rf.currentTerm
	myID := rf.me
	lastLogIndex := rf.log[len(rf.log)-1].Index
	lastLogTerm := rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()

	// make buffered channel to receive replies from request vote RPCs
	chanReqVoteReplies := make(chan RequestVoteReply, numOfPeers)

	// call RPCs
	for i := 0; i < numOfPeers; i++ {
		if i != myID {
			go rf.sendRequestVoteRPC(chanReqVoteReplies, i, RequestVoteArgs{myTerm, myID, lastLogIndex, lastLogTerm}, &RequestVoteReply{Null, false})
		}
	}

	numOfVotes := 1
	end := false

	// three scenarios for ending the following for loop:
	// (i)   we achieve majority of votes -> main candidate has to become leader
	// (ii)  if we receive a reply with term is greater than current term -> main candidate has to update term and become follower
	// (iii) the main candidate thread asks it to exit
	for {
		select {
		case reply := <-chanReqVoteReplies:
			// if reply reached here, if it is true, nice we gained the vote:
			if reply.VoteGranted {
				numOfVotes++
				if numOfVotes > numOfPeers/2 {
					// we have acheived the majority
					chanHasWonElection <- true // inform the candidate main thread
					end = true                 // end this thread
				}
			} else {
				// check if its term was greater than us, we have to fall back to becoming a follower and update our term
				if reply.Term > myTerm {
					chanUpdatedTerm <- reply.Term // inform this to the main candidate thread
					end = true                    // end this thread
				}
			}

		case <-chanCancelElection:
			end = true // end this thread
		}

		if end {
			break
		}
	}
}

func (rf *Raft) beCandidate() {

	// start leader election

	// initialize state
	rf.mu.Lock()
	rf.currentTerm++    // increment current term
	rf.votedFor = rf.me // vote for self
	myTerm := rf.currentTerm

	rf.persist()
	rf.mu.Unlock()

	// send RequestVote RPCs to all other servers
	chanHasWonElection := make(chan bool)
	chanUpdatedTerm := make(chan int)
	chanCancelElection := make(chan bool)
	go rf.callReqVoteRPCs(chanHasWonElection, chanUpdatedTerm, chanCancelElection)

	// bool to break state's infinite for loop
	exitCandidateState := false

	// get timer for an election timeout
	electionTimer := time.NewTimer(getRandomTimeoutInterval())

	for {
		select {

		// if raft server has been killed
		case <-rf.endRaft:
			exitCandidateState = true

		// we have won election
		case <-chanHasWonElection:
			// STATE TRANSITION: candidate -> leader
			rf.mu.Lock()
			// rf.persist()
			rf.mu.Unlock()
			rf.changeServerStateTo("leader")
			exitCandidateState = true

		// election timeout occured
		case <-electionTimer.C:
			// STATE TRANSITION: candidate -> candidate
			rf.changeServerStateTo("candidate") // restart candidate state
			exitCandidateState = true           // exit the candidate state
			chanCancelElection <- true          // end the wait for ReqVote RPCs

		// we have discovered a leader's heartbeat
		case args := <-rf.appendEntriesArgsCh:

			// reply to append entries
			reply, isHeartbeatValid := rf.getAppendEntriesReply(args)
			rf.appendEntriesReplyCh <- AppendEntriesReply{reply.Term, reply.Success}

			// if the leader is legitimate
			if isHeartbeatValid {
				// STATE TRANSITION: candidate -> follower
				rf.changeServerStateTo("follower")
				exitCandidateState = true
				chanCancelElection <- true
			}

		// we have discovered a higher term from a RequestVote RPC
		case updatedTerm := <-chanUpdatedTerm:

			// update our term
			rf.mu.Lock()
			rf.currentTerm = updatedTerm // vote for self
			// // fmt.Println(rf.me, ":", rf.commitIndex, rf.lastApplied, "(RV_Reply: cT changed from", rf.currentTerm, "to", updatedTerm, ")")
			rf.persist()
			rf.mu.Unlock()

			// STATE TRANSITION: candidate -> follower
			rf.changeServerStateTo("follower") // become follower
			exitCandidateState = true          // exit the candidate state
			chanCancelElection <- true         // end the wait for ReqVote RPCs

		// reply to requestVoteRPCs
		case args := <-rf.requestVoteArgsCh:

			reply := rf.getRequestVoteReply(args)
			rf.requestVoteReplyCh <- RequestVoteReply{reply.Term, reply.VoteGranted}

			if reply.Term > myTerm { // && reply.VoteGranted {
				// STATE TRANSITION: candidate -> follower
				rf.changeServerStateTo("follower") // become follower
				exitCandidateState = true          // exit the candidate state
				chanCancelElection <- true         // end the wait for ReqVote RPCs
			}

		}

		if exitCandidateState {
			break
		}
	}
}

type AppendEntryRPCResponse struct {
	ok     bool
	reply  AppendEntriesReply
	server int
}

// wrapper func around sendAppendEntries(...) to receive RPC reply in a channel
func (rf *Raft) sendAppendEntriesRPC(chanReplies chan AppendEntryRPCResponse, server int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	// // // // fmt.Println("appendEntriesArg[i] is", args)
	ok := rf.sendAppendEntries(server, args, reply)
	chanReplies <- AppendEntryRPCResponse{ok, AppendEntriesReply{reply.Term, reply.Success}, server}
}

func (rf *Raft) getLog(i int) Log {
	if i < 0 {
		return rf.log[0]
	}
	return rf.log[i]
}

func (rf *Raft) getEntriesForServer(i int) []Log {

	beginIndex := rf.nextIndex[i]
	if beginIndex < 0 {
		beginIndex = 0
	}

	entriesShallowCopy := rf.log[beginIndex:]

	entriesDeepCopy := make([]Log, len(entriesShallowCopy))
	copy(entriesDeepCopy, entriesShallowCopy)

	return entriesDeepCopy
}

func (rf *Raft) getAppendEntriesArgs(numOfPeers int, myTerm int, myID int) ([]AppendEntriesArgs, int) {

	appendEntriesArgs := make([]AppendEntriesArgs, numOfPeers)

	rf.mu.Lock()

	logLength := len(rf.log)

	// // // // fmt.Println("In getAppendEntriesArgs", rf.nextIndex)

	for i, _ := range appendEntriesArgs {
		if i != myID {
			appendEntriesArgs[i] = AppendEntriesArgs{
				Term:         myTerm,
				LeaderId:     myID,
				PrevLogIndex: rf.getLog(rf.nextIndex[i] - 1).Index,
				PrevLogTerm:  rf.getLog(rf.nextIndex[i] - 1).Term,
				Entries:      rf.getEntriesForServer(i),
				LeaderCommit: rf.commitIndex,
			}
		}
	}

	rf.mu.Unlock()

	return appendEntriesArgs, logLength
}

func (rf *Raft) handleHeartbeatSuccess(response AppendEntryRPCResponse, logLength int, numOfPeers int) {
	// obtain and defer lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// update nextIndex
	nextIndexToPoint := logLength
	rf.nextIndex[response.server] = nextIndexToPoint

	// update matchIndex
	rf.matchIndex[response.server] = nextIndexToPoint - 1

	// // // fmt.Println("updated state", rf.matchIndex, rf.nextIndex)

	// update commit index if logs are replicated in a majority qourum and log is in our current term
	rf.updateCommitIndex(numOfPeers)

	// apply new commits to our state machine if there are any unapplied commits
	if rf.lastApplied < rf.commitIndex {
		// kick application code in go routine as this can block
		go rf.applyNewCommitsToStateMachine()
	}
}

func (rf *Raft) handleHeartbeatFailure(response AppendEntryRPCResponse) bool {

	fallBackToFollower := false

	// obtain lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// // // // fmt.Printf("handleHeartbeatFailure %d %d\n", rf.currentTerm, response.reply.Term)

	// if the unsuccess is due to higher term leader, we need to fall back to follower
	if rf.currentTerm < response.reply.Term {

		// fallbackToFollower
		fallBackToFollower = true
		return fallBackToFollower

	} else
	// if the unsuccess is due to log inconsistency with the follower
	{
		// // // // fmt.Println("going to decrement")
		// decrement nextindex
		rf.nextIndex[response.server]--

		// continue as leader
		fallBackToFollower = false
		return fallBackToFollower
	}
}

func (rf *Raft) sendHeartbeats(numOfPeers int, myTerm int, myID int, chanUpdatedTerm chan int) {

	// make buffered channel to receive replies from RPCs
	chanReplies := make(chan AppendEntryRPCResponse, numOfPeers-1)

	// take lock and prepare custom msgs for each follower
	appendEntriesArgs, logLength := rf.getAppendEntriesArgs(numOfPeers, myTerm, myID)

	// call RPCs
	for i := 0; i < numOfPeers; i++ {
		if i != myID {
			// // // // // fmt.Println("appendEntriesArg[i] is", appendEntriesArgs[i])
			// a := appendEntriesArgs[i]
			go rf.sendAppendEntriesRPC(chanReplies, i, AppendEntriesArgs{appendEntriesArgs[i].Term, appendEntriesArgs[i].LeaderId, appendEntriesArgs[i].PrevLogIndex, appendEntriesArgs[i].PrevLogTerm, appendEntriesArgs[i].Entries, appendEntriesArgs[i].LeaderCommit}, &AppendEntriesReply{Null, false})
		}
	}

	numOfHeartbeatReplies := 1

	// listen from responses
	for {
		response := <-chanReplies

		numOfHeartbeatReplies++

		// check if hearbeat was okay
		if response.ok {
			if response.reply.Success {
				rf.handleHeartbeatSuccess(response, logLength, numOfPeers)
			} else {
				fallBackToFollower := rf.handleHeartbeatFailure(response)

				if fallBackToFollower {
					// // // // // fmt.Println("fa")
					// send updated term to leader go routine and end this thread
					chanUpdatedTerm <- response.reply.Term
					break
				}
			}

		}

		// if all responses are read, end this thread
		if numOfHeartbeatReplies >= numOfPeers {
			break
		}
	}
}

func (rf *Raft) applyToStateMachine() {
	// this is a persistent go routine kick started by rf.kickStartApplyEntriesCh

	exit := false

	for {

		select {

		case <-rf.endApplyToStateMachineCh:
			exit = true

		case <-rf.kickStartApplyEntriesCh:

			if len(rf.endApplyToStateMachineCh) > 0 {

				<-rf.endApplyToStateMachineCh
				exit = true

			} else {

				// obtain lock
				rf.mu.Lock()

				// if we have no commmitted entries that need to be applied, continue on
				if rf.lastApplied >= rf.commitIndex {
					// // // fmt.Println("nahi kia in", rf.me, rf.lastApplied, rf.commitIndex)
					rf.mu.Unlock()
					continue
				}

				// get a copy of logs to apply
				logsToApply := rf.log[max(1, rf.lastApplied) : rf.commitIndex+1]

				// // logging
				// lA_before := rf.lastApplied

				// update lastApplied
				rf.lastApplied = rf.commitIndex

				// // logging
				// t := rf.currentTerm
				// cI := rf.commitIndex
				// lA_after := rf.lastApplied
				// log := rf.log
				// me := rf.me
				// cS := rf.currentRaftState

				// fmt.Println("s:", printMyID(me, cS), ": [app. to st. mac.] t:", t, "cI:", cI, "lA:", fmt.Sprintf("%d->%d", lA_before, lA_after), log, "(applied", logsToApply, "to state machine)")

				// release lock
				rf.mu.Unlock()

				// apply all unapplied logs in order
				for _, log := range logsToApply {
					rf.applyCh <- ApplyMsg{
						Index:   log.Index,
						Command: log.Command,
					}
				}

			}
		}

		if exit {
			break
		}

	}
}

// we are not using this func now!
func (rf *Raft) periodicallyCheckForCommits(numOfPeers int, endCh chan bool) {

	exit := false

	for {

		select {

		case <-endCh:
			exit = true

		default:
			rf.mu.Lock()

			// update commit index if logs are replicated in a majority qourum and log is in our current term
			rf.updateCommitIndex(numOfPeers)

			// apply new commits to our state machine if there are any unapplied commits
			if rf.lastApplied < rf.commitIndex {

				// get a copy of logs to apply
				logsToApplyShallow := rf.log[max(1, rf.lastApplied) : rf.commitIndex+1]
				logsToApply := make([]Log, len(logsToApplyShallow))
				copy(logsToApply, logsToApplyShallow)

				// update lastApplied
				rf.lastApplied = rf.commitIndex

				// release lock
				rf.mu.Unlock()

				// apply all unapplied logs in order
				for _, log := range logsToApply {
					rf.applyCh <- ApplyMsg{
						Index:   log.Index,
						Command: log.Command,
					}
				}

				// // fmt.Println(me, ":", cI, lA, log, "(applied", logsToApply, " by leader to state machine)")

			} else {
				rf.mu.Unlock()
			}
		}

		if exit {
			break
		}

		time.Sleep(CheckCommittableInterval)
	}
}

func (rf *Raft) updateCommitIndex(numOfPeers int) {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if N > rf.commitIndex {
			// check if the log[N].term == currentTerm
			if rf.log[N].Term == rf.currentTerm {
				// check if majority servers has the log replicated
				if rf.doMajorityServersHaveLogReplicated(N, numOfPeers) {
					rf.commitIndex = N
					break
				}
			}
		}
	}
}

func (rf *Raft) doMajorityServersHaveLogReplicated(N, numOfPeers int) bool {
	numOfServersWithLogStored := 1
	for i, _ := range rf.matchIndex {
		if i != rf.me {
			if rf.matchIndex[i] >= N {
				numOfServersWithLogStored++
			}
		}
	}
	return numOfServersWithLogStored > numOfPeers/2
}

func (rf *Raft) periodicallySendHeartBeats(chanUpdatedTerm chan int, chanStop chan bool, numOfPeers int, myTerm int, myID int) {

	go rf.sendHeartbeats(numOfPeers, myTerm, myID, chanUpdatedTerm)

	// get timer for a heartbeat interval
	heartbeatTimer := time.NewTimer(HeartbeatInterval)

	exit := false

	for {

		select {

		// if a duration of HeartbeatInterval has passed since last heartbeat:
		case <-heartbeatTimer.C:

			// resend heartbeats
			go rf.sendHeartbeats(numOfPeers, myTerm, myID, chanUpdatedTerm)

			// restart timer:
			heartbeatTimer.Reset(HeartbeatInterval)

		// main thread is asking us to stop sending heartbeats
		case <-chanStop:
			exit = true
		}

		if exit {
			break
		}
	}
}

func (rf *Raft) beLeader() {

	// set state
	rf.mu.Lock()
	numOfPeers := len(rf.peers)
	myTerm := rf.currentTerm
	myID := rf.me
	lastLogIndex := len(rf.log) - 1
	// printWarning(fmt.Sprintf("lastlogindex: %d", lastLogIndex))
	// // fmt.Println("new leader:", rf.me, rf.log)

	// leader's volatile state
	rf.nextIndex = getNewIntArray(numOfPeers, lastLogIndex+1)
	// // // // fmt.Println(rf.nextIndex)
	rf.matchIndex = getNewIntArray(numOfPeers, 0)

	// rf.persist()
	rf.mu.Unlock()

	// send RequestVote RPCs to all other servers
	chanUpdatedTerm := make(chan int)
	chanStopHeartbeats := make(chan bool)
	go rf.periodicallySendHeartBeats(chanUpdatedTerm, chanStopHeartbeats, numOfPeers, myTerm, myID)

	// start go routine that periodically checks if entries can be committed (not using now)
	// endCheckCommitsCh := make(chan bool)
	// go rf.periodicallyCheckForCommits(numOfPeers, endCheckCommitsCh)

	exitLeaderState := false

	for {
		select {

		// if raft server has been killed
		case <-rf.endRaft:
			exitLeaderState = true
			chanStopHeartbeats <- true
			// endCheckCommitsCh <- true

		// discovers server with higher term through heartbeat replies
		case newTerm := <-chanUpdatedTerm:

			// change state
			rf.mu.Lock()
			rf.currentTerm = newTerm
			// // fmt.Println(rf.me, ":", rf.commitIndex, rf.lastApplied, "(AE_Reply: cT changed from", rf.currentTerm, "to", newTerm, ")")
			rf.persist()
			rf.mu.Unlock()

			// STATE TRANSITION: leader -> follower
			rf.changeServerStateTo("follower")
			exitLeaderState = true
			chanStopHeartbeats <- true
			// endCheckCommitsCh <- true

		// if the leader gets a heartbeat
		case args := <-rf.appendEntriesArgsCh:

			// reply to append entries
			reply, isHeartbeatValid := rf.getAppendEntriesReply(args)
			rf.appendEntriesReplyCh <- AppendEntriesReply{reply.Term, reply.Success}

			// if the leader is legitimate
			if isHeartbeatValid {

				// STATE TRANSITION: leader -> follower
				rf.changeServerStateTo("follower")
				exitLeaderState = true
				chanStopHeartbeats <- true
				// endCheckCommitsCh <- true
			}

		// reply to requestVoteRPCs
		case args := <-rf.requestVoteArgsCh:

			reply := rf.getRequestVoteReply(args)
			rf.requestVoteReplyCh <- RequestVoteReply{reply.Term, reply.VoteGranted}

			if reply.Term > myTerm { // && reply.VoteGranted {

				// STATE TRANSITION: candidate -> follower
				rf.changeServerStateTo("follower") // become follower
				exitLeaderState = true             // exit the candidate state
				chanStopHeartbeats <- true         // end the wait for ReqVote RPCs
				// endCheckCommitsCh <- true
			}

		}

		if exitLeaderState {
			break
		}
	}
}

// util funcs
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
func getNewIntArray(len int, initVal int) []int {
	arr := make([]int, len)
	for i, _ := range arr {
		arr[i] = initVal
	}
	return arr
}
