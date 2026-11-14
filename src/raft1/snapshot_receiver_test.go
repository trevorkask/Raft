package raft

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"6.5840/raftapi"
	"6.5840/tester1"
)

func TestSnapshotBehindCommittedEntries(t *testing.T) {
	for _, index := range []int{1, 2} {
		rf := &Raft{persister: tester.MakePersister(), currentTerm: 3, votedFor: 0,
			state: candidate, log: []LogEntry{{Term: 0}, {Term: 2, Command: "first"}, {Term: 3, Command: "second"}},
			commitIndex: 2, applyCh: make(chan raftapi.ApplyMsg, 3)}
		wantLog := append([]LogEntry(nil), rf.log...)
		reply := InstallSnapshotReply{}
		rf.InstallSnapshot(&InstallSnapshotArgs{Term: 4, LastIncludedIndex: index,
			LastIncludedTerm: rf.termAt(index), Data: []byte("already committed")}, &reply)
		if !reply.Success || reply.Term != 4 || rf.state != follower || rf.votedFor != -1 {
			t.Fatal("snapshot from a new leader did not update the term")
		}
		if rf.lastIncludedIndex != 0 || rf.commitIndex != 2 || rf.lastApplied != 0 ||
			rf.pendingSnapshot != nil || !reflect.DeepEqual(rf.log, wantLog) {
			t.Fatal("snapshot replaced committed entries waiting for application")
		}
		ps := rf.persister.Checkpoint()
		restarted := &Raft{}
		restarted.readPersist(ps.ReadRaftState())
		if restarted.currentTerm != 4 || restarted.votedFor != -1 ||
			restarted.lastIncludedIndex != 0 || !reflect.DeepEqual(restarted.log, wantLog) || len(ps.ReadSnapshot()) != 0 {
			t.Fatal("ignored snapshot changed persisted log or lost the new term")
		}
		rf.applyCommitted()
		for i, command := range []string{"first", "second"} {
			select {
			case msg := <-rf.applyCh:
				if !msg.CommandValid || msg.SnapshotValid || msg.CommandIndex != i+1 || msg.Command != command {
					t.Fatalf("incorrect application after ignored snapshot: %+v", msg)
				}
			default:
				t.Fatal("committed command was lost")
			}
		}
		if len(rf.applyCh) != 0 {
			t.Fatal("ignored snapshot was delivered to the service")
		}
	}
}

func TestInstallSnapshotState(t *testing.T) {
	for _, match := range []bool{false, true} {
		rf := &Raft{persister: tester.MakePersister(), currentTerm: 3, votedFor: 0,
			log: []LogEntry{{Term: 0}, {Term: 1}, {Term: 2}, {Term: 3, Command: "tail"}}}
		term := 2
		if !match {
			term = 3
		}
		args := InstallSnapshotArgs{Term: 4, LastIncludedIndex: 2, LastIncludedTerm: term, Data: []byte("snapshot")}
		reply := InstallSnapshotReply{}
		rf.InstallSnapshot(&args, &reply)
		if !reply.Success || reply.Term != 4 || rf.state != follower || rf.votedFor != -1 || rf.lastIncludedIndex != 2 || rf.commitIndex != 2 {
			t.Fatal("snapshot did not update follower state")
		}
		wantLength := 1
		if match {
			wantLength = 2
		}
		if len(rf.log) != wantLength || rf.log[0].Term != term || rf.log[0].Command != nil {
			t.Fatal("snapshot retained an incorrect log suffix")
		}
		args.Data[0] = 'X'
		ps := rf.persister.Checkpoint()
		restarted := &Raft{}
		restarted.readPersist(ps.ReadRaftState())
		if restarted.lastIncludedIndex != 2 || restarted.lastApplied != 2 || len(restarted.log) != wantLength || !bytes.Equal(ps.ReadSnapshot(), []byte("snapshot")) {
			t.Fatal("installed snapshot was not persisted independently of the RPC buffer")
		}
		rf.applyCh = make(chan raftapi.ApplyMsg, 2)
		rf.InstallSnapshot(&args, &reply)
		rf.applyCommitted()
		msg := <-rf.applyCh
		if !msg.SnapshotValid || msg.CommandValid || msg.SnapshotIndex != 2 || msg.SnapshotTerm != term || !bytes.Equal(msg.Snapshot, []byte("snapshot")) {
			t.Fatalf("incorrect snapshot message: %+v", msg)
		}
		if len(rf.applyCh) != 0 {
			t.Fatal("duplicate snapshot was applied twice")
		}
		stale := InstallSnapshotArgs{Term: 3, LastIncludedIndex: 10, LastIncludedTerm: 3}
		rf.InstallSnapshot(&stale, &reply)
		if reply.Success || reply.Term != 4 || rf.lastIncludedIndex != 2 {
			t.Fatal("accepted a stale leader's snapshot")
		}
		older := InstallSnapshotArgs{Term: 4, LastIncludedIndex: 1, LastIncludedTerm: 1}
		rf.InstallSnapshot(&older, &reply)
		if !reply.Success || rf.lastIncludedIndex != 2 || rf.pendingSnapshot != nil {
			t.Fatal("older snapshot moved state backward")
		}
	}
}

func TestSnapshotApplicationOrder(t *testing.T) {
	rf := &Raft{persister: tester.MakePersister(), currentTerm: 3, votedFor: -1,
		log: []LogEntry{{Term: 0}, {Term: 3, Command: "first"}}, commitIndex: 1,
		applyCh: make(chan raftapi.ApplyMsg)}
	done := make(chan struct{})
	go func() { rf.applyCommitted(); close(done) }()
	deadline := time.Now().Add(time.Second)
	for {
		rf.mu.Lock()
		started := rf.lastApplied == 1
		rf.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("applier did not start")
		}
		time.Sleep(time.Millisecond)
	}
	for _, index := range []int{3, 5} {
		rf.InstallSnapshot(&InstallSnapshotArgs{Term: 3, LastIncludedIndex: index, LastIncludedTerm: 3, Data: []byte{byte(index)}}, &InstallSnapshotReply{})
	}
	rf.AppendEntries(&AppendEntriesArgs{Term: 3, PrevLogIndex: 5, PrevLogTerm: 3, Entries: []LogEntry{{Term: 3, Command: "after"}}, LeaderCommit: 6}, &AppendEntriesReply{})
	for i, index := range []int{1, 5, 6} {
		select {
		case msg := <-rf.applyCh:
			if i == 1 {
				if !msg.SnapshotValid || msg.SnapshotIndex != index {
					t.Fatalf("incorrect snapshot order: %+v", msg)
				}
			} else if !msg.CommandValid || msg.CommandIndex != index {
				t.Fatalf("incorrect command order: %+v", msg)
			}
		case <-time.After(time.Second):
			t.Fatal("application blocked or skipped a message")
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("applier did not finish")
	}
}
