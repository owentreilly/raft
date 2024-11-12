package raft

// Collaborators: None, except quick check ins with TAs or other people in the lab class with help debugging an issue
// like my code deadlocking, or incorrectly truncating the log. The other thing I discussed was a high-level discussion around the lifecycle of the servers and how to avoid writing recursively spiraling cases.
// Still, it was a high level discussion
// as specified in the Piazza post. I am firmly confident in my ability to explain or replicate any part of this assignment. :)
//
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
	"cs350/labgob"
	"cs350/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type LogEntry struct {
	Command interface{}
	Term    int
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//persistent state on all servers
	currentTerm int        //latest term server has seen
	votedFor    int        //candidateId that received vote in current term
	log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (indexed at 1)
	status      string     //Leader, Follower, Candidate
	applyMsg    chan ApplyMsg
	//volatile state on all servers
	commitIndex int       //index of highest log entry known to be committed across servers
	lastApplied int       //index of highest log entry applied to state machine
	lastReset   time.Time //time of the last HB/appendEntries received
	//volatile state on leaders
	nextIndex  []int //for each server, index of the next log entry to send to that server
	matchIndex []int //for each server, index of highest log entry known to be replicated on server
}

func randomTimeout() time.Duration {
	return time.Duration(500+rand.Intn(500)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == "Leader" {
		isleader = true
	} else {
		isleader = false
	}

	term = rf.currentTerm
	return term, isleader
}
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2B).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor := 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		fmt.Println("Error decoding data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = len(rf.log) - 1
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //candidate's term
	CandidateId int //candidate requesting vote
	LastApplied int //index of candidate's last log entry
	LastLogTerm int //term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	rf.mu.Lock()
	fmt.Println("Server ", rf.me, " received request vote from server ", args.CandidateId)
	defer rf.mu.Unlock()

	if rf.currentTerm >= args.Term {
		//candidate has old term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Println("Server ", rf.me, " rejected vote for server, candidate old term ", args.CandidateId)
		return
	}
	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		//already voted for someone else
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return

	}
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId { //already voted for candidate this term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		fmt.Println("Server ", rf.me, " already voted for server ", args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm { //candidate has higher term, revert to follower
		rf.status = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.VoteGranted = false
	}
	lessthanZero := false
	if rf.lastApplied-1 < 0 {
		fmt.Println("Last applied is less than 0")
		lessthanZero = true

	}
	if !lessthanZero {
		isLogBehind := rf.lastApplied > args.LastApplied && rf.log[rf.lastApplied-1].Term == args.LastLogTerm
		isTermBehind := rf.log[rf.lastApplied-1].Term > args.LastLogTerm

		if isLogBehind || isTermBehind {
			// candidate doesn't have an up-to-date log (as defined in the paper)
			reply.VoteGranted = false
			fmt.Println("Server ", rf.me, " rejected vote for server, out of date log ", args.CandidateId)
			reply.Term = args.Term
			return
		}
	}
	fmt.Println("Last applied: ", rf.lastApplied)

	//no conditions met, free to vote for candidate
	rf.status = "Follower"
	reply.Term = args.Term
	rf.votedFor = args.CandidateId
	fmt.Println("Server ", rf.me, " voting for server ", args.CandidateId)
	reply.VoteGranted = true
	rf.lastReset = time.Now()
	rf.persist()
}

func (rf *Raft) processReplyEntry(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entry)
		rf.nextIndex[peer] = min(rf.nextIndex[peer]+len(args.Entry), rf.lastApplied+1)
	} else {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peer] = reply.ConflictLength
			return
		}
		if reply.Term > args.Term {
			rf.status = "Follower"
			return
		}
		index := -1
		for i, value := range rf.log {
			if value.Term == reply.ConflictTerm {
				index = i
			}
		}
		if index == -1 {
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			rf.nextIndex[peer] = index
		}
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        //leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entry        []LogEntry //log entries to store (empty for heartbeat, may send more than one for efficiency)
	LeaderCommit int        //leader's commitIndex
}

type AppendEntriesReply struct {
	Term           int  //currentTerm, for leader to update itself
	Success        bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex  int  //index of the first conflicting entry (-1 if no conflict)
	ConflictLength int  //length of the conflicting array
	ConflictTerm   int  //term of the first conflicting entry if mismatch
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1 //default
	reply.ConflictLength = len(rf.log)
	reply.ConflictIndex = -1

	if args.Term < rf.currentTerm {
		//leader has old term, reject
		fmt.Println("Leader has old term, rejected from server ", rf.me)
		return
	}
	if len(rf.log) < args.PrevLogIndex+1 {
		//Reply false if log doesn’t contain an entry at prevLogIndex
		//whose term matches prevLogTerm
		fmt.Println("Log does not contain entry at prevLogIndex, rejected from server ", rf.me)
		return
	}

	//check to see if the log entry at prevLogIndex matches the term for quick rollback

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		for index, logEntry := range rf.log {
			if rf.log[args.PrevLogIndex].Term == logEntry.Term {
				reply.ConflictIndex = index // returns first conflicting index
				break
			}
		}
		return
	}
	finalIndex := 0
	for index, _ := range args.Entry {
		currentIndex := args.PrevLogIndex + 1 + index
		finalIndex = index
		if len(rf.log) <= currentIndex { // out of bounds
			break
		}

		if rf.log[currentIndex].Term != args.Entry[index].Term { // truncating log from the point of mismatch
			rf.log = rf.log[:currentIndex]
			rf.lastApplied = currentIndex - 1
			rf.persist()
			break
		}
	}
	//reached point of successful append
	rf.lastReset = time.Now()
	reply.Success = true

	if len(args.Entry) > 0 {
		rf.log = append(rf.log, args.Entry[finalIndex:]...)
		fmt.Println("Server ", rf.me, " received appendEntries from leader ", args.LeaderId)
		rf.lastApplied = len(rf.log) - 1
		rf.persist()
	}

	if args.LeaderCommit <= rf.commitIndex {
		return
	}

	commitLimit := min(args.LeaderCommit, rf.lastApplied)
	for commitIndex := rf.commitIndex + 1; commitIndex <= commitLimit; commitIndex++ {
		rf.commitIndex = commitIndex
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[commitIndex].Command,
			CommandIndex: commitIndex,
		}
		fmt.Println("Server ", rf.me, " sending ", msg, " applyMsg to applyCh")
		rf.applyMsg <- msg // send to apply channel
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.status == "Leader")
	if !isLeader {
		return 0, 0, false
	}
	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	rf.lastApplied = len(rf.log) - 1
	index = rf.lastApplied
	rf.persist()
	return index, rf.currentTerm, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) repeat() {
	//cycle for managing the state of the server, repeats every 100ms
	for !rf.killed() {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == "Leader" {
			rf.Leader()
		} else if status == "Candidate" {
			rf.Candidate()
		} else if status == "Follower" {
			rf.Follower()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) Follower() {
	randomDuration := randomTimeout()
	time.Sleep(randomDuration)
	rf.mu.Lock()
	lastReset := rf.lastReset

	if time.Since(lastReset).Milliseconds() >= randomDuration.Milliseconds() {

		rf.currentTerm++
		rf.status = "Candidate"
		fmt.Println("Server ", rf.me, " timed out, starting election")
		rf.votedFor = rf.me
		rf.persist()

	}
	rf.mu.Unlock()
}

func (rf *Raft) Candidate() {
	randomTimeout := randomTimeout()
	start := time.Now()
	//reset election timer for candidate
	rf.mu.Lock()
	//code in this critical section to reduce lock contention for the rest of the function
	me := rf.me
	peers := rf.peers
	isLeader := false
	totalPeers := len(rf.peers)
	term := rf.currentTerm
	successfulElection := false
	lastApplied := rf.lastApplied
	lastLogTerm := rf.log[lastApplied].Term
	rf.mu.Unlock()
	voteCount := 0

	numDone := 0 //to keep track of goroutines that have finished
	for peer := range peers {
		if me == peer {
			rf.mu.Lock()
			numDone++
			voteCount++
			rf.mu.Unlock()
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{}
			args.Term = term
			args.LastLogTerm = lastLogTerm
			args.CandidateId = me
			args.LastApplied = lastApplied
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				numDone++
				fmt.Println("Error sending request vote to server ", peer)
				return
			}
			if reply.VoteGranted == true {
				voteCount++
				numDone++

			} else {
				numDone++
				if args.Term < reply.Term {
					//candidate has old term, revert to follower
					rf.status = "Follower"
					fmt.Println("Server ", rf.me, " has old term, reverting to follower")
					rf.currentTerm = reply.Term
					rf.persist()
				}
			}
		}(peer)
	}

	for {
		rf.mu.Lock()
		if numDone == totalPeers || voteCount >= (totalPeers/2)+1 || time.Since(start).Milliseconds() >= randomTimeout.Milliseconds() {

			break
		}
		rf.mu.Unlock()
		//fmt.Println("Waiting for majority votes, releasing lock")
		//released lock to avoid deadlock
		time.Sleep(50 * time.Millisecond)
	}

	if time.Since(start).Milliseconds() >= randomTimeout.Milliseconds() {
		//election timeout, restart election at a later time
		rf.mu.Lock()
		rf.status = "Follower"
		fmt.Println("Server ", rf.me, " timed out, restarting election")
		rf.mu.Unlock()
		return
	}

	if voteCount >= (totalPeers/2)+1 {
		successfulElection = true
	}

	if rf.status == "Candidate" && successfulElection == true {
		//candidate has majority votes, wins election
		rf.status = "Leader"
		if isLeader == false {
			isLeader = true
		}

		fmt.Println("Server ", rf.me, " won election")
		for i := range peers {
			rf.nextIndex[i] = rf.lastApplied + 1
		}
	} else {
		rf.status = "Follower"
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) Leader() {

	rf.mu.Lock()
	me := rf.me
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	lastApplied := rf.lastApplied
	peers := rf.peers
	nextIndex := rf.nextIndex

	//initialize nextIndex and matchIndex
	matchIndex := rf.matchIndex
	matchIndex[me] = lastApplied
	nextIndex[me] = lastApplied + 1

	log := rf.log
	rf.mu.Unlock()
	for i := commitIndex + 1; i <= lastApplied; i++ {
		count := 0
		totalPeers := len(peers)
		majority := (totalPeers / 2) + 1
		for peer := range peers {
			if matchIndex[peer] >= i && log[i].Term == term {
				count++
			}
		}

		if count >= majority { //majority of servers have log entry at index n, we can safely commit
			rf.mu.Lock()

			for j := rf.commitIndex + 1; j <= i; j++ {
				fmt.Println("Server ", rf.me, " committing log entry at index ", j)
				rf.applyMsg <- ApplyMsg{

					CommandValid: true,
					Command:      log[j].Command,
					CommandIndex: j,
				}
				rf.commitIndex = rf.commitIndex + 1
			}
			rf.mu.Unlock()
		}
	}

	for peer := range peers {
		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		rf.mu.Lock()
		args.Term = rf.currentTerm

		args.LeaderId = rf.me
		prevLogIndex := nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.log[prevLogIndex].Term
		args.LeaderCommit = rf.commitIndex

		if nextIndex[peer] <= lastApplied { //populate args.Entry with log entries between nextIndex and lastApplied
			args.Entry = rf.log[prevLogIndex+1 : lastApplied+1]
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		//send appendEntries RPC to all peers
		go func(peer int) {
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				fmt.Println("Error sending appendEntries to server ", peer)
				return
			}

			rf.mu.Lock()
			if reply.Success {
				rf.matchIndex[peer] = prevLogIndex + len(args.Entry)
				//update matchIndex and nextIndex
				rf.nextIndex[peer] = min(rf.nextIndex[peer]+len(args.Entry), rf.lastApplied+1)

			} else {
				if reply.ConflictTerm == -1 { //no conflict term, just update nextIndex
					rf.nextIndex[peer] = reply.ConflictLength
					rf.mu.Unlock()
					return
				} else if reply.Term > args.Term { //leader has old term, revert to follower
					rf.status = "Follower"
					fmt.Println("Server ", rf.me, " has old term, reverting to follower")
					rf.mu.Unlock()
					return
				}
				index := -1
				for i, value := range rf.log {
					if value.Term == reply.ConflictTerm {
						index = i
					}
				}
				if index == -1 {
					rf.nextIndex[peer] = reply.ConflictIndex
					//conflict detected, update nextIndex accordingly
				} else {
					rf.nextIndex[peer] = index
				}
			}
			rf.mu.Unlock()
			//fmt.Println("releasing lock for leader process")
		}(peer)
	}
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

// make a new raft server
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.status = "Follower"
	rf.peers = peers
	rf.persister = persister
	// Your initialization code here (2A, 2B).
	rf.log = []LogEntry{
		{
			Command: nil,
			Term:    0,
		},
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyMsg = applyCh
	rf.votedFor = -1 //no votes cast yet
	// initialize from persister
	rf.readPersist(persister.ReadRaftState())

	//begin repeat cycle for overseeing server state
	go rf.repeat()
	return rf
}
