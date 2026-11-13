package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state             role
	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastIncludedIndex int
	snapshot          []byte
	pendingSnapshot   *raftapi.ApplyMsg
	nextIndex         []int
	matchIndex        []int
	commitIndex       int
	lastApplied       int
	applyCh           chan raftapi.ApplyMsg
	electionDeadline  time.Time
	heartbeatDeadline time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil || e.Encode(rf.lastIncludedIndex) != nil {
		panic("cannot encode Raft state")
	}
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	d := labgob.NewDecoder(bytes.NewBuffer(data))
	var term, votedFor, lastIncludedIndex int
	var entries []LogEntry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil || d.Decode(&lastIncludedIndex) != nil {
		panic("cannot decode Raft state")
	}
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = entries
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.lastApplied || index > rf.commitIndex {
		return
	}
	offset := index - rf.lastIncludedIndex
	entries := make([]LogEntry, len(rf.log)-offset)
	entries[0].Term = rf.log[offset].Term
	copy(entries[1:], rf.log[offset+1:])
	rf.log = entries
	rf.lastIncludedIndex = index
	rf.snapshot = append([]byte(nil), snapshot...)
	rf.persist()
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	lastIndex := rf.lastLogIndex()
	lastTerm := rf.termAt(lastIndex)
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	if upToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if rf.votedFor != args.CandidateId {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.VoteGranted = true
		rf.resetElectionDeadline()
	}
}

func (rf *Raft) resetElectionDeadline() {
	rf.electionDeadline = time.Now().Add(time.Duration(450+rand.Intn(300)) * time.Millisecond)
}

func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = follower
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = rf.lastLogIndex() + 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.becomeFollower(args.Term)
	rf.resetElectionDeadline()
	reply.Term = rf.currentTerm
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}
	if args.PrevLogIndex > rf.lastLogIndex() {
		return
	}
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex > rf.lastIncludedIndex && rf.termAt(reply.ConflictIndex-1) == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		return
	}
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index > rf.lastLogIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		if rf.termAt(index) != entry.Term {
			rf.log = append(rf.log[:index-rf.lastIncludedIndex], args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	commit := args.LeaderCommit
	if last := args.PrevLogIndex + len(args.Entries); commit > last {
		commit = last
	}
	if commit > rf.commitIndex {
		rf.commitIndex = commit
	}
	reply.Success = true
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.peers {
		rf.nextIndex[peer] = rf.lastLogIndex() + 1
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.heartbeatDeadline = time.Time{}
}

func (rf *Raft) startElection() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionDeadline()
	lastIndex := rf.lastLogIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  rf.termAt(lastIndex),
	}
	votes := 1
	if votes > len(rf.peers)/2 {
		rf.becomeLeader()
		return
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.resetElectionDeadline()
				return
			}
			if rf.state != candidate || rf.currentTerm != args.Term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}(peer)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.heartbeatDeadline = time.Now().Add(120 * time.Millisecond)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateTo(peer)
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.becomeFollower(args.Term)
	rf.resetElectionDeadline()
	reply.Term = rf.currentTerm
	reply.Success = true
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	entries := []LogEntry{{Term: args.LastIncludedTerm}}
	if args.LastIncludedIndex <= rf.lastLogIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		entries = append(entries, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]...)
	}
	rf.log = entries
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshot = append([]byte(nil), args.Data...)
	rf.persist()
	rf.pendingSnapshot = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      append([]byte(nil), rf.snapshot...),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) handleSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionDeadline()
		return false
	}
	if rf.state != leader || rf.currentTerm != args.Term || !reply.Success {
		return false
	}
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	return true
}

func (rf *Raft) replicateTo(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		next := rf.nextIndex[peer]
		if next <= rf.lastIncludedIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.log[0].Term,
				Data:              append([]byte(nil), rf.snapshot...),
			}
			rf.mu.Unlock()
			reply := InstallSnapshotReply{}
			if !rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply) {
				return
			}
			rf.mu.Lock()
			retry := rf.handleSnapshotReply(peer, &args, &reply)
			rf.mu.Unlock()
			if !retry {
				return
			}
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.termAt(next - 1),
			Entries:      append([]LogEntry(nil), rf.log[next-rf.lastIncludedIndex:]...),
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		if !rf.peers[peer].Call("Raft.AppendEntries", &args, &reply) {
			return
		}
		rf.mu.Lock()
		retry := rf.handleAppendReply(peer, &args, &reply)
		rf.mu.Unlock()
		if !retry {
			return
		}
	}
}

func (rf *Raft) handleAppendReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionDeadline()
		return false
	}
	if rf.state != leader || rf.currentTerm != args.Term {
		return false
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
			rf.nextIndex[peer] = match + 1
		}
		rf.advanceCommit()
		return false
	}
	next := args.PrevLogIndex + 1
	if rf.nextIndex[peer] != next {
		return false
	}
	backup := reply.ConflictIndex
	if reply.ConflictTerm == -1 && backup > next {
		if backup > rf.lastLogIndex()+1 {
			backup = rf.lastLogIndex() + 1
		}
		rf.nextIndex[peer] = backup
		return backup != next
	}
	if next <= rf.matchIndex[peer]+1 {
		return false
	}
	if reply.ConflictTerm >= 0 {
		for index := rf.lastLogIndex(); index >= rf.lastIncludedIndex; index-- {
			if rf.termAt(index) == reply.ConflictTerm {
				backup = index + 1
				break
			}
		}
	}
	if backup >= next {
		backup = next - 1
	}
	if backup < rf.matchIndex[peer]+1 {
		backup = rf.matchIndex[peer] + 1
	}
	rf.nextIndex[peer] = backup
	return true
}

func (rf *Raft) advanceCommit() {
	if rf.state != leader {
		return
	}
	for index := rf.lastLogIndex(); index > rf.commitIndex; index-- {
		if rf.termAt(index) != rf.currentTerm {
			continue
		}
		votes := 0
		for _, match := range rf.matchIndex {
			if match >= index {
				votes++
			}
		}
		if votes > len(rf.peers)/2 {
			rf.commitIndex = index
			return
		}
	}
}

func (rf *Raft) applyCommitted() {
	for {
		rf.mu.Lock()
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			rf.lastApplied = msg.SnapshotIndex
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			return
		}
		rf.lastApplied++
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
	}
}

func (rf *Raft) applier() {
	for {
		rf.applyCommitted()
		time.Sleep(10 * time.Millisecond)
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
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return -1, rf.currentTerm, false
	}
	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.advanceCommit()
	rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		if rf.state == leader {
			if !time.Now().Before(rf.heartbeatDeadline) {
				rf.broadcastAppendEntries()
			}
		} else if !time.Now().Before(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = follower
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.resetElectionDeadline()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
