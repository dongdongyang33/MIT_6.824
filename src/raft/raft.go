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
	"log"
	"math/rand"
	"sort"
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
	CommandIndex int
	Command      interface{}
}

type Log struct {
	term int
	msg  ApplyMsg
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

type Status struct {
	isLeader bool
	index    int
	term     int
}

type VoteStatus struct {
	votingTerm  int
	votingCount int
}

type ClientAppendRequest struct {
	logCommand interface{}
	replyCh    chan Status
}

type NotifyMsg struct {
	messageType int
	msg         interface{}
}

type AppendEntriesReplyWithId struct {
	peerid int
	reply  *AppendEntriesReply
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
	role                   int
	majority               int
	electionTimout         int
	heartbeatTimout        int
	electionTimoutCounter  int
	heartbeatTimoutCounter int
	votingCounter          int
	notifyCh               chan NotifyMsg

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	ch := make(chan Status)
	rf.sendResultToRoutine(4, ch)

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
	Term         int
	Candidateid  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	GrantVote bool
	Term      int
}

type AppendEntriesArgs struct {
	Term         int
	Leaderid     int
	PrevLogIndex int
	PrevLogTerm  int
	CommitIndex  int
	Entries      []Log
}

type AppendEntriesReply struct {
	AppendSuccess         bool
	Term                  int
	LastLogIndex          int
	LastLogTerm           int
	LastLogTermFirstIndex int
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

	rf.sendResultToRoutine(5, &para)

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
	rf.electionTimoutCounter = 0
	rf.heartbeatTimoutCounter = 0
	rf.electionTimout = 250
	rf.heartbeatTimout = 100
	rf.notifyCh = make(chan NotifyMsg, 100)
	rf.votingCounter = 0

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
						para := notify.msg.(*RequestVotePara)
						rf.handleRequestVote(para)
					}
				case 1: // handle AppendEntries RPC
					{
						para := notify.msg.(*AppendEntriesPara)
						rf.handleAppendEntries(para)
					}

				case 2: // handle the RequesstVote reply
					{
						reply := notify.msg.(*RequestVoteReply)
						rf.handleRequestVoteReply(reply)
					}

				case 3: // handle the AppendEntries reply
					{
						reply := notify.msg.(*AppendEntriesReplyWithId)
						rf.handleAppendEntriesReply(reply)
					}

				case 4: // handle getStatus request
					{
						//ch := make(chan Status)
						replyCh := notify.msg.(chan Status)
						rf.handleGetStatusRequest(replyCh)
					}

				case 5: // handle append request from client
					{
						msg := notify.msg.(ClientAppendRequest)
						rf.handleClientAppendMsg(msg)
					}

				case 6: // handle the time count from timer
					{
						reply := notify.msg.(int)
						rf.handleTimerNotify(reply)
					}
				default:

				}
			}
		}
	}
}

func (rf *Raft) raftTimer() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.sendResultToRoutine(6, 10)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	para := RequestVotePara{}
	ch := make(chan *RequestVoteReply)
	para.args = args
	para.reply = reply
	para.replyCh = ch

	rf.sendResultToRoutine(0, &para)

	reply = <-ch
}

func (rf *Raft) handleRequestVote(para *RequestVotePara) {
	para.reply.GrantVote = false
	para.reply.Term = rf.term

	if rf.term < para.args.Term || (rf.term == para.args.Term && rf.votefor == -1) {
		rf.becomeFollower(para.args.Term, -1)
		currentIndex, currentTerm := rf.getLastLogInfo()
		if currentTerm < para.args.LastLogTerm || (currentTerm == para.args.LastLogTerm && currentIndex <= para.args.LastLogIndex) {
			rf.becomeFollower(para.args.Term, para.args.Candidateid)
			para.reply.GrantVote = true
			para.reply.Term = para.args.Term
			log.Println("[server %v] Vote for server %v in term %v", rf.me, rf.votefor, rf.term)
		}
	}
	para.replyCh <- para.reply
}

func (rf *Raft) startRequestVote() {
	rf.becomeCandidate()
	log.Println("[server %v] election timeout. Begin request vote with term %v", rf.me, rf.term)

	args := RequestVoteArgs{}
	args.Term = rf.term
	args.Candidateid = rf.me
	args.LastLogIndex, args.LastLogTerm = rf.getLastLogInfo()

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendPeerRequestVote(i, &args)
		}
	}
}

func (rf *Raft) sendPeerRequestVote(peerid int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.peers[peerid].Call("Raft.RequestVote", args, &reply)
	if !ok {
		reply.GrantVote = false
		reply.Term = -1
	}
	rf.sendResultToRoutine(2, &reply)
}

func (rf *Raft) handleRequestVoteReply(reply *RequestVoteReply) {
	if rf.term < reply.Term {
		rf.becomeFollower(reply.Term, -1)
		log.Println("[server %v] become follower by receiving vote reply with term %v", rf.me, rf.term)
	} else {
		if reply.Term == rf.term && reply.GrantVote { // vote for me.current term
			rf.votingCounter += 1
			if rf.votingCounter >= rf.majority && rf.role == 1 {
				rf.becomeLeader()
			}
		}
	}
}

func (rf *Raft) startAppendEntries(isHeartbeat bool) {
	// TODO: think again how to re-write this function
	rf.heartbeatTimoutCounter = 0

	args := AppendEntriesArgs{}
	args.Term = rf.term
	args.Leaderid = rf.me
	args.CommitIndex = rf.commitIndex
	if isHeartbeat {
		args.Entries = nil
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
	}

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendPeerAppendEntries(isHeartbeat, i, args)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	para := AppendEntriesPara{}
	ch := make(chan *AppendEntriesReply)
	para.args = args
	para.reply = reply
	para.replyCh = ch

	rf.sendResultToRoutine(1, &para)

	reply = <-ch
}

func (rf *Raft) sendPeerAppendEntries(isHeartbeat bool, peerid int, args AppendEntriesArgs) {
	if !isHeartbeat {
		// TODO: update the args by using next[]
		nextIndex := rf.next[peerid]
		args.PrevLogIndex = nextIndex - 1
		args.PrevLogTerm = rf.log[nextIndex-1].term
		args.Entries = rf.log[nextIndex:]
	}
	reply := AppendEntriesReply{}
	ok := rf.peers[peerid].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		reply.AppendSuccess = false
		reply.Term = -1
	}
	replyWithId := AppendEntriesReplyWithId{}
	replyWithId.peerid = peerid
	replyWithId.reply = &reply
	rf.sendResultToRoutine(3, &replyWithId)
}

func (rf *Raft) handleAppendEntries(para *AppendEntriesPara) {
	para.reply.AppendSuccess = false
	para.reply.Term = rf.term

	if rf.term <= para.args.Term {
		rf.becomeFollower(para.args.Term, para.args.Leaderid)
		para.reply.Term = rf.term
		if para.args.PrevLogIndex == -1 && para.args.PrevLogTerm == -1 { // heartbeat
			para.reply.AppendSuccess = true
			para.reply.LastLogIndex = -1
		} else { // appendEntries
			currentLogIndex, currentLogTerm := rf.getLastLogInfo()

			if currentLogIndex > para.args.PrevLogIndex {
				// if log longer than leader, truncate
				// need to find the truncate point
				rf.log = rf.log[:para.args.PrevLogIndex+1]
				currentLogIndex, currentLogTerm = rf.getLastLogInfo()
			}

			if currentLogIndex == para.args.PrevLogIndex && currentLogTerm == para.args.PrevLogTerm {
				para.reply.AppendSuccess = true
				rf.log = append(rf.log, para.args.Entries...)
				para.reply.LastLogIndex, _ = rf.getLastLogInfo()
			} else {
				para.reply.LastLogIndex = currentLogIndex
				para.reply.LastLogTerm = currentLogTerm
				para.reply.LastLogTermFirstIndex = rf.findTheFirstIndex(currentLogIndex)
			}
		}
	}
}

func (rf *Raft) findTheFirstIndex(index int) int {
	currentLastTerm := rf.log[index].term
	ret := index
	for i := index - 1; i > 0; i-- {
		if rf.log[i].term != currentLastTerm {
			ret = i
			break
		}
	}
	return ret
}

func (rf *Raft) handleAppendEntriesReply(replywithid *AppendEntriesReplyWithId) {
	reply := replywithid.reply
	peerid := replywithid.peerid
	if rf.term < reply.Term {
		rf.becomeFollower(reply.Term, -1)
		log.Println("[server %v] become follower by receiving append reply with term %v", rf.me, rf.term)
	} else {
		// TODO: re-think about reply.
		if rf.term == reply.Term {
			if reply.AppendSuccess {
				if reply.LastLogIndex != -1 { // not a heartbeat reply
					rf.match[peerid] = reply.LastLogIndex
					rf.next[peerid] = reply.LastLogIndex + 1
					currentIndex, _ := rf.getLastLogInfo()
					if currentIndex > rf.next[peerid] {
						rf.next[peerid]++
					}
					rf.updateCommitIndex()
				}
				// need to update commitindex at the same time
			} else {
				// TODO: make back
				rf.next[peerid]--
			}
			// how to update next[] and match[]
			// next[] first and if next[] ok then update match []
			// and when match change, update commitidex
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	sortslice := rf.match[:]
	sort.Sort(sortslice)
	commitPoint := len(rf.peers) - rf.majority
	commitPointTerm := rf.log[commitPoint].term
	if (commitPointTerm == rf.term) && (rf.commitIndex < commitPoint) {
		rf.commitIndex = commitPoint
		log.Printf("[server %v] commit index update to %v", rf.me, rf.commitIndex)
		// TODO: append logical
	} else {
		// TODO:
	}
}

func (rf *Raft) handleTimerNotify(tick int) {
	rf.electionTimoutCounter += tick
	rf.heartbeatTimoutCounter += tick

	if rf.role == 2 {
		// leader - send heartbeat or appendentries is timeout
		// TODO: how to decide which msg leader should send
		if rf.heartbeatTimoutCounter >= rf.heartbeatTimoutCounter {
			rf.startAppendEntries(true)
		}
	} else {
		// follower/candidate - begin election
		if rf.electionTimoutCounter >= rf.electionTimout {

			rf.electionTimoutCounter = 0
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			rf.electionTimout = int(r.Int31n(150)) + 250

			rf.startRequestVote()
		}
	}
}

func (rf *Raft) handleGetStatusRequest(replyCh chan Status) {
	reply := Status{}
	reply.term = rf.term
	if rf.role == 2 {
		reply.isLeader = true
	} else {
		reply.isLeader = false
	}
	// TODO: re-think here
	reply.index, _ = rf.getLastLogInfo()

	replyCh <- reply
}

func (rf *Raft) handleClientAppendMsg(msg ClientAppendRequest) {
	reply := Status{}
	reply.isLeader = false
	if rf.role == 2 {
		appendLog := Log{}
		appendLog.term = rf.term
		appendLog.msg.Command = msg.logCommand

		rf.log = append(rf.log, appendLog)
		reply.isLeader = true
	}

	reply.index, reply.term = rf.getLastLogInfo()

	msg.replyCh <- reply
}

func (rf *Raft) getLastLogInfo() (int, int) {
	var index, term int
	loglen := len(rf.log)
	if loglen == 0 {
		index = 0
		term = 0
	} else {
		index = loglen - 1
		term = rf.log[index].term
	}

	return index, term
}

func (rf *Raft) sendResultToRoutine(msgType int, msg interface{}) {
	notify := NotifyMsg{}
	notify.messageType = msgType
	notify.msg = msg

	rf.notifyCh <- notify
}

func (rf *Raft) becomeFollower(receiveTerm int, receiveVotefor int) {
	rf.role = 0
	rf.term = receiveTerm
	rf.votefor = receiveVotefor
	rf.votingCounter = 0
	rf.electionTimoutCounter = 0
}

func (rf *Raft) becomeCandidate() {
	rf.role = 1
	rf.term += 1
	rf.votefor = rf.me
	rf.votingCounter = 1
	rf.electionTimoutCounter = 0
}

func (rf *Raft) becomeLeader() {
	rf.role = 2
	rf.heartbeatTimoutCounter = 0
	lastindex, _ := rf.getLastLogInfo()
	for i, _ := range rf.peers {
		rf.match[i] = 0
		rf.next[i] = lastindex
	}
	rf.startAppendEntries(true)
}
