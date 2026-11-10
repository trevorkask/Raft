package raft

import (
	"6.5840/labrpc"
	"6.5840/tester1"
	"reflect"
	"testing"
)

func TestLogOffsetPersistence(t *testing.T) {
	rf := &Raft{
		persister: tester.MakePersister(), currentTerm: 4, votedFor: 1,
		lastIncludedIndex: 4,
		log:               []LogEntry{{Term: 2}, {Term: 3, Command: "e"}},
	}
	rf.persist()
	restarted := &Raft{}
	restarted.readPersist(rf.persister.ReadRaftState())
	if restarted.lastIncludedIndex != 4 || restarted.lastLogIndex() != 5 ||
		restarted.lastApplied != 4 || restarted.commitIndex != 4 ||
		restarted.currentTerm != 4 || restarted.votedFor != 1 || !reflect.DeepEqual(restarted.log, rf.log) {
		t.Fatal("restart lost the retained log's indexes or state")
	}
}

func TestLogOffsetElection(t *testing.T) {
	rf := &Raft{
		persister:   tester.MakePersister(),
		currentTerm: 4, votedFor: -1,
		lastIncludedIndex: 4,
		log:               []LogEntry{{Term: 2}, {Term: 3, Command: "e"}, {Term: 3, Command: "f"}},
	}
	reply := RequestVoteReply{}
	rf.RequestVote(&RequestVoteArgs{Term: 4, CandidateId: 1, LastLogIndex: 4, LastLogTerm: 3}, &reply)
	if reply.VoteGranted {
		t.Fatal("compacted follower voted for a shorter log")
	}
	rf.RequestVote(&RequestVoteArgs{Term: 4, CandidateId: 1, LastLogIndex: 6, LastLogTerm: 3}, &reply)
	if !reply.VoteGranted {
		t.Fatal("compacted follower rejected an up-to-date log")
	}
}

func TestLogOffsetReplication(t *testing.T) {
	fullLog := []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 1, Command: "b"}, {Term: 2, Command: "c"}, {Term: 2, Command: "d"}, {Term: 3, Command: "e"}, {Term: 3, Command: "f"}}
	makePeer := func(base, last int) *Raft {
		entries := append([]LogEntry(nil), fullLog[base:last+1]...)
		entries[0].Command = nil
		return &Raft{persister: tester.MakePersister(), currentTerm: 4, votedFor: -1, lastIncludedIndex: base, log: entries, commitIndex: base, lastApplied: base}
	}
	tests := []struct {
		name         string
		leaderBase   int
		followerBase int
		followerLast int
		next         int
	}{
		{"leader compacted further", 4, 2, 4, 7},
		{"follower compacted further", 2, 4, 6, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network := labrpc.MakeNetwork()
			defer network.Cleanup()
			end := network.MakeEnd("replication")
			f := makePeer(tt.followerBase, tt.followerLast)
			server := labrpc.MakeServer()
			server.AddService(labrpc.MakeService(f))
			network.AddServer("follower", server)
			network.Connect("replication", "follower")
			network.Enable("replication", true)
			rf := makePeer(tt.leaderBase, 6)
			rf.peers = []*labrpc.ClientEnd{nil, end}
			rf.becomeLeader()
			rf.nextIndex[1] = tt.next
			rf.replicateTo(1)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lastLogIndex() != 6 || rf.matchIndex[1] != 6 || rf.nextIndex[1] != 7 {
				t.Fatal("replication lost absolute log indexes")
			}
			if !reflect.DeepEqual(f.log[1:], fullLog[tt.followerBase+1:]) {
				t.Fatalf("unexpected follower log: %+v", f.log)
			}
			if count := network.GetTotalCount(); count > 2 {
				t.Fatalf("replication used %d RPCs, want at most 2", count)
			}
		})
	}
}
