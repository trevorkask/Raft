package raft

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

func TestSnapshotReplyProgress(t *testing.T) {
	rf := &Raft{persister: tester.MakePersister(), state: leader, currentTerm: 4, votedFor: 0,
		log: []LogEntry{{Term: 0}}, nextIndex: []int{1, 2}, matchIndex: []int{0, 1}}
	args := InstallSnapshotArgs{Term: 4, LastIncludedIndex: 5}
	if rf.handleSnapshotReply(1, &args, &InstallSnapshotReply{Term: 4}) || rf.nextIndex[1] != 2 {
		t.Fatal("rejected snapshot advanced replication")
	}
	if !rf.handleSnapshotReply(1, &args, &InstallSnapshotReply{Term: 4, Success: true}) || rf.nextIndex[1] != 6 || rf.matchIndex[1] != 5 {
		t.Fatal("accepted snapshot did not advance replication")
	}
	args.LastIncludedIndex = 3
	rf.handleSnapshotReply(1, &args, &InstallSnapshotReply{Term: 4, Success: true})
	if rf.nextIndex[1] != 6 || rf.matchIndex[1] != 5 {
		t.Fatal("delayed snapshot reply moved replication backward")
	}
	args.Term = 3
	args.LastIncludedIndex = 7
	if rf.handleSnapshotReply(1, &args, &InstallSnapshotReply{Term: 3, Success: true}) || rf.matchIndex[1] != 5 {
		t.Fatal("old leadership reply changed replication progress")
	}
	rf.handleSnapshotReply(1, &args, &InstallSnapshotReply{Term: 5})
	if rf.state != follower || rf.currentTerm != 5 || rf.votedFor != -1 {
		t.Fatal("higher term reply did not cause step down")
	}
}

func TestLocalSnapshot(t *testing.T) {
	rf := &Raft{
		persister:   tester.MakePersister(),
		currentTerm: 3,
		votedFor:    1,
		log:         []LogEntry{{Term: 0}, {Term: 1, Command: strings.Repeat("a", 4096)}, {Term: 2, Command: "b"}, {Term: 2, Command: "c"}, {Term: 3, Command: "d"}, {Term: 3, Command: "e"}},
		commitIndex: 4,
		lastApplied: 4,
	}
	rf.persist()
	oldSize := rf.PersistBytes()
	oldLog := rf.log
	snapshot := []byte("state through index 3")
	rf.Snapshot(3, snapshot)
	wantLog := []LogEntry{{Term: 2}, {Term: 3, Command: "d"}, {Term: 3, Command: "e"}}
	if rf.lastIncludedIndex != 3 || rf.lastLogIndex() != 5 || !reflect.DeepEqual(rf.log, wantLog) {
		t.Fatalf("compacted log = %+v at index %d", rf.log, rf.lastIncludedIndex)
	}
	if &rf.log[0] == &oldLog[3] || rf.PersistBytes() >= oldSize {
		t.Fatal("compaction retained the old log storage")
	}
	snapshot[0] = 'X'
	if !bytes.Equal(rf.snapshot, []byte("state through index 3")) {
		t.Fatal("snapshot shares the caller's buffer")
	}
	for _, index := range []int{2, 3, 5, 6} {
		rf.Snapshot(index, []byte("invalid replacement"))
	}
	if rf.lastIncludedIndex != 3 || !reflect.DeepEqual(rf.log, wantLog) {
		t.Fatal("stale or unapplied snapshot changed the log")
	}
	rf.becomeFollower(4)
	ps := rf.persister.Checkpoint()
	restarted := &Raft{persister: ps, snapshot: ps.ReadSnapshot()}
	restarted.readPersist(ps.ReadRaftState())
	if restarted.currentTerm != 4 || restarted.lastIncludedIndex != 3 || restarted.commitIndex != 3 || restarted.lastApplied != 3 || !reflect.DeepEqual(restarted.log, wantLog) {
		t.Fatal("restart lost the snapshot boundary or retained entries")
	}
	if !bytes.Equal(restarted.snapshot, []byte("state through index 3")) {
		t.Fatal("later persistence lost the saved snapshot")
	}
	restarted.applyCh = make(chan raftapi.ApplyMsg, 2)
	restarted.commitIndex = 5
	restarted.applyCommitted()
	for i, command := range []string{"d", "e"} {
		select {
		case msg := <-restarted.applyCh:
			if !msg.CommandValid || msg.CommandIndex != i+4 || msg.Command != command {
				t.Fatalf("unexpected command after restart: %+v", msg)
			}
		default:
			t.Fatal("retained command was not applied")
		}
	}
	restarted.Snapshot(5, []byte("state through index 5"))
	if len(restarted.log) != 1 || restarted.log[0].Term != 3 || restarted.log[0].Command != nil {
		t.Fatal("snapshot of the last entry did not leave a term-only boundary")
	}
	restarted.peers = make([]*labrpc.ClientEnd, 1)
	restarted.becomeLeader()
	if index, _, ok := restarted.Start("f"); !ok || index != 6 {
		t.Fatalf("Start after compaction returned index %d, leader %t", index, ok)
	}
}
