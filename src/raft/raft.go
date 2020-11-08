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
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	term    int
	message ApplyMsg
}

// for routine to handle requestVote RPC
type RequestVotePara struct {
	args    *RequestVoteArgs
	reply   *RequestVoteReply
	replyCh chan *RequestVoteReply
}

// for routine to handle appnedEntries RPC
type AppendEntriesPara struct {
	args    *AppendEntriesArgs
	reply   *AppendEntriesReply
	replyCh chan *AppendEntriesReply
}

type VoteResult struct {
	becomeLeader bool
	term         int
}

type AppendResult struct {
	appendSuccess bool
	term          int
}

type Status struct {
	term     int
	isLeader bool
	index    int
}

type ClientAppendRequest struct {
	logCommand interface{}
	replyCh    chan Status
}

type NotifyMsg struct {
	messageType int
	msg         interface{}

	//	requestVotePara      *RequestVotePara   // 0
	//	appendEntriesPara    *AppendEntriesPara // 1
	//	voteResult           *VoteResult        // 2
	//	appednResult         *AppendResult      // 3
	//	statusMsg            *StatusMsg         // 4: get status
	//  clientAppendRequest  ClientAppendRequest // 5
	//  timer int // 6
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

	// 2A + 2B - for all server and must be persisten
	term    int
	votefor int
	log     []Log

	// 2A + 2B - for all server
	commitIndex  int
	appliedIndex int

	// 2A + 2B - for leader only
	match []int
	next  []int

	// additional parameter
	role            int
	notifyCh        chan NotifyMsg
	timerCh         chan int
	majority        int
	lastLogIndex    int
	tick            int
	electionTimout  int
	heartbeatTimout int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	notify := NotifyMsg{}
	ch := make(chan Status)
	notify.messageType = 4
	notify.msg = &ch

	rf.notifyCh <- notify
	status := <-ch

	term = status.term
	isleader = status.isLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateid  int
	lastLogTerm  int
	lastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	grantVote bool
	term      int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	ch := make(chan *RequestVoteReply)
	para := RequestVotePara{}
	para.args = args
	para.reply = reply
	para.replyCh = ch

	notify := NotifyMsg{}
	notify.messageType = 0
	notify.msg = &para

	rf.notifyCh <- notify

	reply = <-ch
}

type AppendEntriesArgs struct {
	term         int
	leaderid     int
	prevLogIndex int
	prevLogTerm  int
	entries      []Log
	commitIndex  int
}

type AppendEntriesReply struct {
	appendSuccess bool
	term          int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ch := make(chan *AppendEntriesReply)
	para := AppendEntriesPara{}
	para.args = args
	para.reply = reply
	para.replyCh = ch

	notify := NotifyMsg{}
	notify.messageType = 1
	notify.msg = &para

	rf.notifyCh <- notify

	reply = <-ch
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

	// Your code here (2B).
	ch := make(chan Status)
	para := ClientAppendRequest{}
	para.logCommand = command
	para.replyCh = ch

	notify := NotifyMsg{}
	notify.messageType = 6

	rf.notifyCh <- notify

	status := <-ch

	index = status.index
	term = status.term
	isLeader = status.isLeader

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.term = 0
	rf.votefor = -1

	// 2B - all server
	rf.commitIndex = 0
	rf.appliedIndex = 0

	// 2B - leader
	peerNum := len(rf.peers)
	rf.match = make([]int, peerNum)
	rf.next = make([]int, peerNum)

	// additional para
	rf.role = 0
	rf.majority = peerNum/2 + 1
	rf.tick = 10
	rf.electionTimout = 0
	rf.heartbeatTimout = 0
	rf.notifyCh = make(chan NotifyMsg, 100)
	rf.timerCh = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.raftRoutine()

	return rf
}

func (rf *Raft) raftRoutine() {
	go rf.raftTimer()
	for {
		select {
		case notify := <-rf.notifyCh:
			{
				msgtype := notify.messageType
				switch msgtype {
				case 0: // handle RequestVote RPC
					{
						para := notify.msg.(RequestVotePara)
						rf.handleRequestVote(&para)
					}
				case 1: // handle AppendEntries RPC
					{
						para := notify.msg.(AppendEntriesPara)
						rf.handleAppendEntries(&para)
					}

				case 2: // handle the RequesstVote result which send out by myself

				case 3: // handle the AppendEntries result which send out by myself

				case 4: // handle getStatus request

				case 5: // handle append request from client

				case 6: // handle the time count from timer

				default:

				}
			}
		}
	}
}

func (rf *Raft) raftTimer() {
	for {
		time.Sleep(rf.tick * time.Millisecond)
		notify := NotifyMsg{}
		notify.messageType = 7
		notify.msg = rf.tick
		rf.notifyCh <- notify
	}
}

func (rf *Raft) handleRequestVote(para *RequestVotePara) {
	para.reply.grantVote = false
	para.reply.term = rf.term

	if rf.term < para.args.term || (rf.term == para.args.term && rf.votefor == -1) {
		currentIndex, currentTerm := rf.getLastLogInfo()
		if currentTerm < para.args.lastLogTerm || (currentTerm == para.args.lastLogTerm && currentIndex <= para.args.lastLogIndex) {
			rf.term = para.args.term
			rf.votefor = para.args.candidateid
			rf.electionTimout = 0
			para.reply.grantVote = true
		}
	}
	para.replyCh <- para.reply
}

func (rf *Raft) handleAppendEntries(para *AppendEntriesPara) {
	para.reply.appendSuccess = false
	para.reply.term = rf.term

	if rf.term <= para.args.term {
		rf.term = para.args.term
		rf.role = 0
		rf.votefor = para.args.leaderid
		rf.electionTimout = 0

		// compare the log and try to append
		// return success  = true if it can append

	}
}

func (rf *Raft) getLastLogInfo() (int, int) {
	index := len(rf.log) - 1
	term := rf.log[index].term

	return index, term
}
