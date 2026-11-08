package raft

import (
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
)

func TestCommitMajority(t *testing.T) {
	tests := []struct {
		name      string
		term      int
		state     role
		matches   []int
		committed int
		want      int
	}{
		{"minority", 3, leader, []int{4, 0, 0}, 0, 0},
		{"older term majority", 3, leader, []int{4, 2, 2}, 0, 0},
		{"current term commits prefix", 3, leader, []int{4, 3, 2}, 0, 3},
		{"latest entry majority", 3, leader, []int{4, 4, 2}, 0, 4},
		{"half is not a majority", 3, leader, []int{4, 4, 0, 0}, 0, 0},
		{"new leader waits for current term", 4, leader, []int{4, 4, 4}, 2, 2},
		{"follower does not count replicas", 3, follower, []int{4, 4, 4}, 2, 2},
		{"commit never decreases", 3, leader, []int{4, 0, 0}, 3, 3},
		{"single peer", 3, leader, []int{4}, 0, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := &Raft{
				peers:       make([]*labrpc.ClientEnd, len(tt.matches)),
				state:       tt.state,
				currentTerm: tt.term,
				log:         []LogEntry{{Term: 0}, {Term: 1}, {Term: 2}, {Term: 3}, {Term: 3}},
				matchIndex:  tt.matches,
				commitIndex: tt.committed,
			}
			rf.advanceCommit()
			if rf.commitIndex != tt.want {
				t.Fatalf("commit index = %d, want %d", rf.commitIndex, tt.want)
			}
		})
	}
}

func TestFollowerCommitBounds(t *testing.T) {
	tests := []struct {
		name        string
		args        AppendEntriesArgs
		committed   int
		want        int
		wantSuccess bool
	}{
		{"heartbeat commits matched prefix", AppendEntriesArgs{Term: 3, PrevLogIndex: 2, PrevLogTerm: 2, LeaderCommit: 3}, 0, 2, true},
		{"leader commit limits progress", AppendEntriesArgs{Term: 3, PrevLogIndex: 3, PrevLogTerm: 3, LeaderCommit: 1}, 0, 1, true},
		{"old commit does not move backward", AppendEntriesArgs{Term: 3, PrevLogIndex: 3, PrevLogTerm: 3, LeaderCommit: 1}, 2, 2, true},
		{"mismatch does not commit", AppendEntriesArgs{Term: 3, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 3}, 0, 0, false},
		{"old leader does not commit", AppendEntriesArgs{Term: 2, PrevLogIndex: 2, PrevLogTerm: 2, LeaderCommit: 3}, 0, 0, false},
		{"new entries can commit", AppendEntriesArgs{Term: 3, PrevLogIndex: 3, PrevLogTerm: 3, Entries: []LogEntry{{Term: 3, Command: "d"}}, LeaderCommit: 4}, 2, 4, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := &Raft{
				currentTerm: 3,
				log:         []LogEntry{{Term: 0}, {Term: 1}, {Term: 2}, {Term: 3}},
				commitIndex: tt.committed,
			}
			reply := AppendEntriesReply{}
			rf.AppendEntries(&tt.args, &reply)
			if reply.Success != tt.wantSuccess || rf.commitIndex != tt.want {
				t.Fatalf("success = %t, commit index = %d; want %t, %d", reply.Success, rf.commitIndex, tt.wantSuccess, tt.want)
			}
		})
	}
}

func TestApplyCommittedOrder(t *testing.T) {
	applyCh := make(chan raftapi.ApplyMsg)
	rf := &Raft{
		currentTerm: 3,
		log:         []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 2, Command: "b"}, {Term: 3, Command: "c"}, {Term: 3, Command: "uncommitted"}},
		commitIndex: 2,
		applyCh:     applyCh,
	}
	done := make(chan struct{})
	go func() {
		rf.applyCommitted()
		close(done)
	}()
	for i, command := range []string{"a", "b", "c"} {
		if i == 1 {
			updated := make(chan struct{})
			go func() {
				rf.GetState()
				rf.mu.Lock()
				rf.commitIndex = 3
				rf.mu.Unlock()
				close(updated)
			}()
			select {
			case <-updated:
			case <-time.After(time.Second):
				t.Fatal("blocked application prevented access to Raft state")
			}
		}
		select {
		case msg := <-applyCh:
			if !msg.CommandValid || msg.SnapshotValid || msg.CommandIndex != i+1 || msg.Command != command {
				t.Fatalf("apply message = %+v, want command %s at index %d", msg, command, i+1)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing apply message for index %d", i+1)
		}
	}
	select {
	case <-done:
	case msg := <-applyCh:
		t.Fatalf("applied an uncommitted entry: %+v", msg)
	case <-time.After(time.Second):
		t.Fatal("application did not finish")
	}
	done = make(chan struct{})
	go func() {
		rf.applyCommitted()
		close(done)
	}()
	select {
	case <-done:
	case msg := <-applyCh:
		t.Fatalf("applied an entry twice: %+v", msg)
	case <-time.After(time.Second):
		t.Fatal("application did not finish on an unchanged commit index")
	}
}
