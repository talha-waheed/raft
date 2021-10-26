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
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// heartbeat and election timeout in ms
const MinTimeout = 300
const MaxTimeout = 600

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
	state string

	// channels to recieve information from RPCs
	chanAppendEntriesArgs  chan AppendEntriesArgs
	chanAppendEntriesReply chan AppendEntriesReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
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
	// if args.Term < currentTerm:
	// set reply.votegranted = false
	// set reply.Term = currentTerm

	// else if votedFor == nil or votedFor == candidateId:
	// also some other logic we gotta deal with in ass3
	// get mutex and change apna term and vote
	// set reply.votegranted = true
	// set reply.Term = args.Term
	//
	// else:
	// set reply.votegranted = false
	// set reply.Term = currentTerm
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
	rf.votedFor = 0
	rf.state = "follower"
	rf.chanAppendEntriesArgs = make(chan AppendEntriesArgs)
	rf.chanAppendEntriesReply = make(chan AppendEntriesReply)
	rf.mu.Unlock()

	// on startup, start by being a follower
	go rf.beFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func printWarning(toPrint string) {
	fmt.Println("Warning: " + toPrint)
}

func (rf *Raft) changeServerStateTo(newServerState string) {
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
		reply.Term = rf.currentTerm
	} else {
		// if this is the same or greater term, then this is the new leader, iski bait karo
		// inform leader that we agree to it being the leader
		reply.Success = true
		reply.Term = args.Term
		// change our state to agree to the leader
		rf.currentTerm = args.Term
		rf.votedFor = Null
	}

	// the calling function (beFollower, beCandidate, or beLeader) will alter its behavior by inspecting this reply
	// e.g. the candidate will fall back to become a follower if reply.Success == true
	return reply
}

func (rf *Raft) beFollower() {

	exitFollowerState := false

	// get timer for a heartbeat interval
	heartbeatTimer := time.NewTimer(getRandomTimeoutInterval())

	for {

		select {

		// if heartbeat times out
		case <-heartbeatTimer.C:
			// STATE TRANSITION: follower -> candidate
			exitFollowerState = true            // exit the follower state
			rf.changeServerStateTo("candidate") // become a candidate

		// if we get a heartbeat
		case appendEntriesArgs := <-rf.chanAppendEntriesArgs:

			// reply to append entries
			reply := rf.getAppendEntriesReply(appendEntriesArgs)
			rf.chanAppendEntriesReply <- AppendEntriesReply{reply.Term, reply.Success}

			// as a follower, there is nothing to do with the reply, except to restart timer:
			// 1. stop timer
			if !heartbeatTimer.Stop() {
				// just ensuring that if the channel has value,
				// drain it before restarting (so we dont leak resources
				// for keeping the channel indefinately up)
				<-heartbeatTimer.C
			}
			// 2. reset timer
			heartbeatTimer.Reset(getRandomTimeoutInterval())
		}

		if exitFollowerState {
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
	rf.mu.Unlock()

	// - send RequestVote RPCs to all other servers (in go channels)
	// - if they reply with success channel main daalo kek

	// number of votes = 1

	// - either you
	// 	1. receive a RequestVoteRPC result:
	// 		- if success:
	// 			numOfVotes++
	// 			if numVotes >= majority:
	// 				make_self_leader()
	// 		- else:
	// 			- fall_back_to_follower()
	// 			- end this thread
	// 	2. timeout:
	// 		- go startLeaderElection()
	// 		- end this thread
	// 	3. append entries rpc value:
	// 		- fall_back_to_follower()
	// 		- end this thread

}

func (rf *Raft) beLeader() {}

/*
func make_self_leader() {
	yay im leader, make a channel to leave this thread when we are no longer leader
	now i will update my state
	then
	either
		in every 1/10 s i will do this:
			send appendentries
			if success, good.
			if not, fall_back_to_follower(term) and append to channel that we are not leader
		or someone appended to our channel that we are no longer leader:
			fallbacktofollower(term)
}
*/

/*
func fallbacktofollower(){
	endelection and fallback to follower state
	go wait_for_heartbeat ()
}
*/
