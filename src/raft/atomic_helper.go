package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) getCommitIndex() uint64 {
	return atomic.LoadUint64(&rf.commitIndex)
}

func (rf *Raft) setCommitIndex(index uint64) {
	atomic.StoreUint64(&rf.commitIndex, index)
}

func (rf *Raft) getLastApplied() uint64 {
	return atomic.LoadUint64(&rf.lastApplied)
}

func (rf *Raft) setLastApplied(index uint64) {
	atomic.StoreUint64(&rf.lastApplied, index)
}

func (rf *Raft) getLeaderID() int {
	return int(atomic.LoadInt32(&rf.leaderId))
}

func (rf *Raft) setLeaderID(peerId int) {
	atomic.StoreInt32(&rf.leaderId, int32(peerId))
}

func (rf *Raft) getVotedFor() int {
	return int(atomic.LoadInt32(&rf.votedFor))
}

func (rf *Raft) setVotedFor(peerId int) {
	atomic.StoreInt32(&rf.votedFor, int32(peerId))
}

func (rf *Raft) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&rf.currentTerm)
}

func (rf *Raft) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&rf.currentTerm, term)
}

func (rf *Raft) getLastContactTime() time.Time {
	rf.lastContactLock.RLock()
	last := rf.lastContactTime
	rf.lastContactLock.RUnlock()
	return last
}

func (rf *Raft) setLastContact(t time.Time) {
	rf.lastContactLock.Lock()
	rf.lastContactTime = t
	rf.lastContactLock.Unlock()
}

func (rf *Raft) getState() State {
	stateAddr := (*uint32)(&rf.state)
	return State(atomic.LoadUint32(stateAddr))
}

func (rf *Raft) setState(s State) {
	stateAddr := (*uint32)(&rf.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}
