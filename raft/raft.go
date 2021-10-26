package raft

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
	"fmt"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// heartbeat and election timeout in ms
const MinTimeout = 300
const MaxTimeout = 600

// heartbeat interval
const HeartbeatInterval = time.Second / 10

const Null int = -1

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
	currentTerm int
	votedFor    int

	// volatile state
	isLeader bool

	// channels to recieve information from RPCs
	chanAppendEntriesArgs  chan AppendEntriesArgs
	chanAppendEntriesReply chan AppendEntriesReply
	chanRequestVoteArgs    chan RequestVoteArgs
	chanRequestVoteReply   chan RequestVoteReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// LastLogIndex
	// LastLogTerm
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// send the rpc request over to the follower/candidate/leader go routine running rn
	rf.chanRequestVoteArgs <- args

	// it will examine server states and reply with the reply
	replyFromChan := <-rf.chanRequestVoteReply

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
	Term     int
	LeaderId int
	// PrevLogIndex
	// PrevLogTerm
	// Entries[]
	// LeaderCommit
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	// send the rpc request over to the follower/candidate/leader go routine running rn
	rf.chanAppendEntriesArgs <- args

	// it will examine server states and reply with the reply
	replyFromChan := <-rf.chanAppendEntriesReply

	// send the reply received back
	reply.Success = replyFromChan.Success
	reply.Term = replyFromChan.Term
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index = rf.me
	term = rf.currentTerm
	isLeader = rf.isLeader
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
	// Your code here, if desired.
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

	// initialize our (volatile) state
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = Null
	rf.isLeader = false
	rf.chanAppendEntriesArgs = make(chan AppendEntriesArgs)
	rf.chanAppendEntriesReply = make(chan AppendEntriesReply)
	rf.chanRequestVoteArgs = make(chan RequestVoteArgs)
	rf.chanRequestVoteReply = make(chan RequestVoteReply)
	rf.mu.Unlock()

	// on startup, start by being a follower
	go rf.beFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func printWarning(toPrint string) {
	// fmt.Println("Warning: " + toPrint)
}

func (rf *Raft) changeServerStateTo(newServerState string) {

	rf.mu.Lock()
	myID := rf.me
	rf.mu.Unlock()

	if newServerState == "follower" {
		go rf.beFollower()
	} else if newServerState == "candidate" {
		printWarning(fmt.Sprintf("candidate %d", myID))
		go rf.beCandidate()
	} else if newServerState == "leader" {
		go rf.beLeader()
	} else {
		printWarning(fmt.Sprintf("%s is an invalid state", newServerState))
	}
}

func getRandomTimeoutInterval() time.Duration {
	return time.Duration(rand.Intn(MaxTimeout-MinTimeout)+MinTimeout) * time.Millisecond
}

func (rf *Raft) getAppendEntriesReply(args AppendEntriesArgs) AppendEntriesReply {

	// wrap the execution of this func in locks
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := AppendEntriesReply{}

	if rf.currentTerm > args.Term {
		// current term gt sender's term, reject this msg
		reply.Success = false
		// and tell leader to update its term to rf.currentTerm
		reply.Term = rf.currentTerm
	} else {
		// if this is the same or greater term, then this is the new leader, iski bait karo
		// inform leader that we agree to it being the leader
		reply.Success = true
		reply.Term = args.Term
		// change our term to conform to the leader
		rf.currentTerm = args.Term
		// if we had vote for anyone, clear it. We are beyond candidate state
		rf.votedFor = Null
	}

	// the calling function (beFollower, beCandidate, or beLeader) will alter its behavior by inspecting this reply
	// e.g. the candidate will fall back to become a follower if reply.Success == true
	return reply
}

func (rf *Raft) getRequestVoteReply(args RequestVoteArgs) RequestVoteReply {

	// wrap the execution of this func in locks
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := RequestVoteReply{}

	if args.Term < rf.currentTerm {
		// if current term is greater than the requestvote term, send the currentTerm back
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		// if reqvote term is equal to or greater than currentTerm, grant vote if
		// (i) we haven't voted for anyone yet, and (ii) we have voted for this candidate before
		if rf.votedFor == Null || rf.votedFor == args.CandidateId {
			// inform reqester that we are voting for it
			reply.Term = int(math.Max(float64(args.Term), float64(rf.currentTerm)))
			reply.VoteGranted = true
			// change our state to vote for the player
			// // note that earlier I was not changing our term here,
			rf.votedFor = args.CandidateId
			rf.currentTerm = int(math.Max(float64(args.Term), float64(rf.currentTerm)))
		} else
		// if we have already voted
		{
			if args.Term > rf.currentTerm {
				// if current term is greater than the requestvote term, send the currentTerm back
				reply.Term = args.Term
				reply.VoteGranted = true

				rf.votedFor = args.CandidateId
				rf.currentTerm = args.Term
			} else {
				// if current term is greater than the requestvote term, send the currentTerm back
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}

		}
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
	rf.isLeader = false
	myID := rf.me
	rf.mu.Unlock()

	// bool to break state's infinite for loop
	exitFollowerState := false

	// get timer for a heartbeat interval
	heartbeatTimer := time.NewTimer(getRandomTimeoutInterval())
	t1 := time.Now()

	for {

		select {

		// if heartbeat times out
		case <-heartbeatTimer.C:
			// STATE TRANSITION: follower -> candidate
			printWarning(fmt.Sprintf("hearbeat timeout %d with elapsed: %d", myID, time.Since(t1)/1000000))
			exitFollowerState = true            // exit the follower state
			rf.changeServerStateTo("candidate") // become a candidate

		// if we get a heartbeat
		case args := <-rf.chanAppendEntriesArgs:

			// reply to append entries
			reply := rf.getAppendEntriesReply(args)
			rf.chanAppendEntriesReply <- AppendEntriesReply{reply.Term, reply.Success}

			// as a follower, there is nothing to do with the reply, except to restart timer

			// if legitimate heartbeat
			if reply.Success {
				// restart timer (as we received a heartbeat!):
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

		// reply to requestVoteRPCs
		case args := <-rf.chanRequestVoteArgs:

			reply := rf.getRequestVoteReply(args)
			rf.chanRequestVoteReply <- RequestVoteReply{reply.Term, reply.VoteGranted}

			// if legitimate heartbeat
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
	rf.mu.Unlock()

	// make buffered channel to receive replies from request vote RPCs
	chanReqVoteReplies := make(chan RequestVoteReply, numOfPeers)

	// call RPCs
	for i := 0; i < numOfPeers; i++ {
		if i != myID {
			go rf.sendRequestVoteRPC(chanReqVoteReplies, i, RequestVoteArgs{myTerm, myID}, &RequestVoteReply{Null, false})
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

// Note: I'm ignoring RequestVote RPCs in candidate state
func (rf *Raft) beCandidate() {

	// start leader election

	// initialize state
	rf.mu.Lock()
	rf.isLeader = false
	rf.currentTerm++    // increment current term
	rf.votedFor = rf.me // vote for self
	myTerm := rf.currentTerm
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

		// we have won election
		case <-chanHasWonElection:
			// STATE TRANSITION: candidate -> leader
			rf.mu.Lock()
			rf.isLeader = true
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
		case args := <-rf.chanAppendEntriesArgs:

			// reply to append entries
			reply := rf.getAppendEntriesReply(args)
			rf.chanAppendEntriesReply <- AppendEntriesReply{reply.Term, reply.Success}

			// if the leader is legitimate
			if reply.Success {
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
			rf.mu.Unlock()

			// STATE TRANSITION: candidate -> follower
			rf.changeServerStateTo("follower") // become follower
			exitCandidateState = true          // exit the candidate state
			chanCancelElection <- true         // end the wait for ReqVote RPCs

		// reply to requestVoteRPCs
		case args := <-rf.chanRequestVoteArgs:

			reply := rf.getRequestVoteReply(args)
			rf.chanRequestVoteReply <- RequestVoteReply{reply.Term, reply.VoteGranted}

			if reply.Term > myTerm {
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
	ok    bool
	reply AppendEntriesReply
}

// wrapper func around sendAppendEntries(...) to receive RPC reply in a channel
func (rf *Raft) sendAppendEntriesRPC(chanReplies chan AppendEntryRPCResponse, server int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)
	chanReplies <- AppendEntryRPCResponse{ok, AppendEntriesReply{reply.Term, reply.Success}}
}

func (rf *Raft) sendHeartbeats(numOfPeers int, myTerm int, myID int, chanUpdatedTerm chan int) {

	// make buffered channel to receive replies from RPCs
	chanReplies := make(chan AppendEntryRPCResponse, numOfPeers-1)

	// call RPCs
	for i := 0; i < numOfPeers; i++ {
		if i != myID {
			go rf.sendAppendEntriesRPC(chanReplies, i, AppendEntriesArgs{myTerm, myID}, &AppendEntriesReply{Null, false})
		}
	}

	numOfHeartbeatReplies := 1

	// listen from responses
	for {
		response := <-chanReplies

		numOfHeartbeatReplies++

		// if we receive a heartbeat message responging unsucessfully, update our term and become follower
		if response.ok && !response.reply.Success {
			chanUpdatedTerm <- response.reply.Term
		}

		// if all responses are read, end this thread
		if numOfHeartbeatReplies >= numOfPeers {
			break
		}
	}
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
			// 1. stop timer
			if !heartbeatTimer.Stop() {
				// just ensuring that if the channel has value,
				// drain it before restarting (so we dont leak resources
				// for keeping the channel indefinately up)
				<-heartbeatTimer.C
			}
			// 2. reset timer
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

// Note: I'm ignoring RequestVote RPCs in leader state
func (rf *Raft) beLeader() {

	// set state
	rf.mu.Lock()
	rf.isLeader = true
	numOfPeers := len(rf.peers)
	myTerm := rf.currentTerm
	myID := rf.me
	rf.mu.Unlock()

	// send RequestVote RPCs to all other servers
	chanUpdatedTerm := make(chan int)
	chanStopHeartbeats := make(chan bool)
	go rf.periodicallySendHeartBeats(chanUpdatedTerm, chanStopHeartbeats, numOfPeers, myTerm, myID)

	exitLeaderState := false

	for {
		select {

		// discovers server with higher term through heartbeat replies
		case newTerm := <-chanUpdatedTerm:

			// change state
			rf.mu.Lock()
			rf.currentTerm = newTerm
			rf.isLeader = false
			rf.mu.Unlock()

			// STATE TRANSITION: leader -> follower
			rf.changeServerStateTo("follower")
			exitLeaderState = true
			chanStopHeartbeats <- true

		// if the leader gets a heartbeat
		case args := <-rf.chanAppendEntriesArgs:

			// reply to append entries
			reply := rf.getAppendEntriesReply(args)
			rf.chanAppendEntriesReply <- AppendEntriesReply{reply.Term, reply.Success}

			// if the leader is legitimate
			if reply.Success {

				// change state
				rf.mu.Lock()
				rf.isLeader = false
				rf.mu.Unlock()

				// STATE TRANSITION: leader -> follower
				rf.changeServerStateTo("follower")
				exitLeaderState = true
				chanStopHeartbeats <- true
			}

		// we have discovered a higher term from a RequestVote RPC
		case updatedTerm := <-chanUpdatedTerm:

			// update our term
			rf.mu.Lock()
			rf.currentTerm = updatedTerm // vote for self
			rf.mu.Unlock()

			// STATE TRANSITION: candidate -> follower
			rf.changeServerStateTo("follower") // become follower
			exitLeaderState = true             // exit the candidate state
			chanStopHeartbeats <- true         // end the wait for ReqVote RPCs

		// reply to requestVoteRPCs
		case args := <-rf.chanRequestVoteArgs:

			reply := rf.getRequestVoteReply(args)
			rf.chanRequestVoteReply <- RequestVoteReply{reply.Term, reply.VoteGranted}

			if reply.Term > myTerm {

				// change state
				rf.mu.Lock()
				rf.isLeader = false
				rf.mu.Unlock()

				// STATE TRANSITION: candidate -> follower
				rf.changeServerStateTo("follower") // become follower
				exitLeaderState = true             // exit the candidate state
				chanStopHeartbeats <- true         // end the wait for ReqVote RPCs
			}

		}

		if exitLeaderState {
			break
		}
	}
}
