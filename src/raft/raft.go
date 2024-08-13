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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	HeartBeatInterval = time.Second / 8
	ElectionTimeout   = 1 * time.Second

	NoneVotedForID = -1
)

type State uint32

const (
	Unknown State = iota
	Follower
	Candidate
	Leader
)

func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := time.Duration(rand.Int63()) % minVal
	return time.After(minVal + extra)
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    uint64
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	lastContactTime time.Time
	lastContactLock sync.RWMutex

	leaderId int32

	hbCh chan struct{}

	currentTerm uint64
	votedFor    int32
	logs        []*LogEntry
	logLock     sync.Mutex

	commitIndex uint64
	lastApplied uint64

	nextIndex  []uint64
	matchIndex []uint64

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.getCurrentTerm()), rf.getState() == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term           uint64
	LeaderId       int
	PrevLogIndex   uint64
	PrevLogTerm    uint64
	Entries        []*LogEntry
	LeaderCommitId uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer func() {
		DPrintf("node:%d receive AppendEntries args:%v reply:%v", rf.me, args, reply)
	}()

	if args.Term < rf.getCurrentTerm() {
		DPrintf("node:%d receive small args.term:%d self term:%d ", rf.me, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.getCurrentTerm()
		return
	}

	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	if int(args.PrevLogIndex) > len(rf.logs)-1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("node:%d receive wrong logs. prevLogInd:%d logsLastTerm:%d prevLogTerm:%d", rf.me,
			args.PrevLogIndex, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.Success = false
		reply.Term = rf.getCurrentTerm()
		return
	}

	rf.setCurrentTerm(args.Term)
	rf.setLeaderID(args.LeaderId)
	if rf.getState() != Follower {
		rf.setState(Follower)
	}

	for i := int(args.PrevLogIndex + 1); i < len(rf.logs); i++ {
		if rf.logs[i].Term != args.PrevLogTerm {
			//delete wrong log entry
			rf.logs = append(rf.logs[:i], rf.logs[i+1:]...)
		}
	}

	for i, entry := range args.Entries {
		if int(args.PrevLogIndex)+1+i > len(rf.logs) {
			rf.logs = append(rf.logs, entry)
		}
	}

	if args.LeaderCommitId > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitId, uint64(len(rf.logs)-1))
	}
	reply.Success = true
	reply.Term = rf.getCurrentTerm()
	rf.setLastContact(time.Now())
	DPrintf("node:%d receive heartbeat success. args:%v reply:%v", rf.me, args, reply)
	return
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint64
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint64
	VoteGranted bool
}

func (rf *Raft) isLogUpToDate(prevLogTerm, prevLogIndex uint64) bool {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()

	lastLogIndex := len(rf.logs) - 1
	if prevLogTerm == rf.logs[lastLogIndex].Term {
		return int(prevLogIndex) >= lastLogIndex
	}

	return prevLogTerm > rf.logs[lastLogIndex].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	defer func() {
		DPrintf("node:%d receive vote args:%v reply:%v", rf.me, args, reply)
	}()

	if args.Term < rf.getCurrentTerm() {
		DPrintf("node:%d receive vote small term:%d current:%d", rf.me, args.Term, rf.currentTerm)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term == rf.getCurrentTerm() && rf.getVotedFor() != NoneVotedForID && rf.getVotedFor() != args.CandidateId {
		DPrintf("node:%d already voted. candidate:%d vote for:%d", rf.me, args.CandidateId, rf.votedFor)
		reply.Term, reply.VoteGranted = rf.getCurrentTerm(), false
		return
	}

	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.getCurrentTerm(), false
		rf.setVotedFor(NoneVotedForID)
		return
	}

	rf.setCurrentTerm(args.Term)
	reply.Term, reply.VoteGranted = rf.getCurrentTerm(), true
	rf.setVotedFor(args.CandidateId)
	rf.setLastContact(time.Now())
	return
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
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	term := -1
	isLeader := true

	// Your code here (3B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, false
	}

	log := &LogEntry{
		Term:    uint64(term),
		Command: command,
	}
	rf.logLock.Lock()
	rf.logs = append(rf.logs, log)
	index = len(rf.logs) - 1
	rf.logLock.Unlock()

	//todo send signal to replicator

	return index, term, isLeader
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

func (rf *Raft) startElection() {
	rf.setState(Candidate)
	rf.setVotedFor(rf.me)
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	DPrintf("{Node %d} starts election with RequestVoteRequest %v", rf.me, args)

	votes := 1
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peerId, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.getCurrentTerm() {
					//become follower
					DPrintf("node:%d term:%d reply term:%d become follower when start election", rf.me, rf.currentTerm,
						reply.Term)
					rf.setCurrentTerm(reply.Term)
					rf.setState(Follower)
					return
				}
				DPrintf("node:%v state:%v send vote receive resp args:%v reply:%v", rf.me, rf.state, args, reply)

				if rf.getCurrentTerm() == reply.Term && reply.VoteGranted {
					votes++
					if votes >= (len(rf.peers)/2)+1 && rf.getState() == Candidate {
						DPrintf("node:%d term:%d votes:%d become leader", rf.me, rf.currentTerm, votes)
						rf.setState(Leader)
						rf.setLeaderID(rf.me)
						rf.doHeartBeat()
					}
				}
			}

		}(peerId)

	}
}

func (rf *Raft) sendAppendEntriesEachPeer(args *AppendEntriesArgs) {
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(peerId, args, reply) {
				if reply.Term > rf.getCurrentTerm() {
					DPrintf("receive peer:%d term:%d larger than current term:%d,become follower", peerId, reply.Term, rf.currentTerm)
					rf.setState(Follower)
					rf.setCurrentTerm(reply.Term)
					return
				}

				if !reply.Success {
					DPrintf("leader:%d send peer:%d heartbeat Failed", rf.leaderId, peerId)
				}
			}
		}(peerId)
	}
}

func (rf *Raft) doHeartBeat() {
	args := &AppendEntriesArgs{
		Term:     rf.getCurrentTerm(),
		LeaderId: rf.me,
	}
	rf.sendAppendEntriesEachPeer(args)
}

func (rf *Raft) startReplicator() {
	for peerId, _ := range rf.peers {
		lastLogIndex := rf.
	}
}

func (rf *Raft) applier() {
	//todo
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)

		switch rf.getState() {
		case Follower:
			for rf.getState() == Follower {
				lastContactTime := rf.getLastContactTime()
				if time.Since(lastContactTime) > ElectionTimeout {
					DPrintf("node:%d hb timeout with last hb time:%v. change to Candidate", rf.me, lastContactTime)
					rf.setState(Candidate)
					break
				}

				ms := 300 + (rand.Int63() % 300)
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		case Candidate:
			for rf.getState() == Candidate {
				rf.startElection()

				timer := randomTimeout(ElectionTimeout)
				select {
				case <-timer:
				}
			}
		case Leader:
			for rf.getState() == Leader {
				select {
				case <-time.After(HeartBeatInterval):
					rf.doHeartBeat()
				}
			}
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		currentTerm:     0,
		commitIndex:     0,
		lastApplied:     0,
		lastContactTime: time.Now(),
		state:           Follower,
		votedFor:        NoneVotedForID,
		logs:            []*LogEntry{{}},
		applyCh:         make(chan ApplyMsg),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
