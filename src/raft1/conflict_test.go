package raft

import (
	"reflect"
	"testing"

	"6.5840/labrpc"
)

func TestConflictRecovery(t *testing.T) {
	makeLog := func(runs ...int) []LogEntry {
		entries := []LogEntry{{Term: 0}}
		for i := 0; i < len(runs); i += 2 {
			for n := 0; n < runs[i+1]; n++ {
				entries = append(entries, LogEntry{Term: runs[i], Command: len(entries)})
			}
		}
		return entries
	}
	tests := []struct {
		name        string
		leaderLog   []LogEntry
		followerLog []LogEntry
		maxRPCs     int
	}{
		{"short log", makeLog(1, 100, 4, 100), makeLog(1, 20), 2},
		{"unknown conflicting terms", makeLog(1, 100, 4, 100), makeLog(1, 20, 2, 150, 3, 40), 3},
		{"known conflicting term", makeLog(1, 20, 2, 80, 4, 100), makeLog(1, 20, 2, 150, 3, 40), 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network := labrpc.MakeNetwork()
			defer network.Cleanup()
			end := network.MakeEnd("replication")
			f := &Raft{currentTerm: 5, votedFor: -1, log: tt.followerLog}
			server := labrpc.MakeServer()
			server.AddService(labrpc.MakeService(f))
			network.AddServer("follower", server)
			network.Connect("replication", "follower")
			network.Enable("replication", true)
			rf := &Raft{peers: []*labrpc.ClientEnd{nil, end}, currentTerm: 5, log: tt.leaderLog}
			rf.becomeLeader()
			rf.replicateTo(1)
			f.mu.Lock()
			replicated := append([]LogEntry(nil), f.log...)
			f.mu.Unlock()
			if !reflect.DeepEqual(replicated, tt.leaderLog) {
				t.Fatal("follower did not recover the leader's log")
			}
			if rf.matchIndex[1] != len(tt.leaderLog)-1 || rf.nextIndex[1] != len(tt.leaderLog) {
				t.Fatalf("incorrect replication progress: match %d, next %d", rf.matchIndex[1], rf.nextIndex[1])
			}
			if calls := network.GetTotalCount(); calls > tt.maxRPCs {
				t.Fatalf("recovery used %d RPCs, want at most %d", calls, tt.maxRPCs)
			}
		})
	}
}

func TestAppendReplyOrdering(t *testing.T) {
	tests := []struct {
		name      string
		next      int
		match     int
		args      AppendEntriesArgs
		reply     AppendEntriesReply
		wantNext  int
		wantMatch int
		wantRetry bool
	}{
		{
			name:     "unknown conflict",
			next:     7,
			args:     AppendEntriesArgs{Term: 5, PrevLogIndex: 6},
			reply:    AppendEntriesReply{Term: 5, ConflictTerm: 3, ConflictIndex: 3},
			wantNext: 3, wantRetry: true,
		},
		{
			name:     "known conflict",
			next:     7,
			args:     AppendEntriesArgs{Term: 5, PrevLogIndex: 6},
			reply:    AppendEntriesReply{Term: 5, ConflictTerm: 2, ConflictIndex: 2},
			wantNext: 5, wantRetry: true,
		},
		{
			name:     "short follower",
			next:     7,
			args:     AppendEntriesArgs{Term: 5, PrevLogIndex: 6},
			reply:    AppendEntriesReply{Term: 5, ConflictTerm: -1, ConflictIndex: 2},
			wantNext: 2, wantRetry: true,
		},
		{
			name: "late rejection after success",
			next: 7, match: 6,
			args:     AppendEntriesArgs{Term: 5, PrevLogIndex: 6},
			reply:    AppendEntriesReply{Term: 5, ConflictTerm: -1, ConflictIndex: 2},
			wantNext: 7, wantMatch: 6,
		},
		{
			name:     "late rejection after backtracking",
			next:     4,
			args:     AppendEntriesArgs{Term: 5, PrevLogIndex: 6},
			reply:    AppendEntriesReply{Term: 5, ConflictTerm: -1, ConflictIndex: 2},
			wantNext: 4,
		},
		{
			name: "late success does not lower progress",
			next: 7, match: 6,
			args:     AppendEntriesArgs{Term: 5, PrevLogIndex: 2, Entries: []LogEntry{{Term: 2}}},
			reply:    AppendEntriesReply{Term: 5, Success: true},
			wantNext: 7, wantMatch: 6,
		},
		{
			name: "previous leadership success",
			next: 5, match: 4,
			args:     AppendEntriesArgs{Term: 4, PrevLogIndex: 6},
			reply:    AppendEntriesReply{Term: 4, Success: true},
			wantNext: 5, wantMatch: 4,
		},
		{
			name: "higher term still causes step down",
			next: 5, match: 4,
			args:     AppendEntriesArgs{Term: 4, PrevLogIndex: 6},
			reply:    AppendEntriesReply{Term: 6},
			wantNext: 5, wantMatch: 4,
		},
		{
			name: "success advances progress",
			next: 5, match: 4,
			args:     AppendEntriesArgs{Term: 5, PrevLogIndex: 4, Entries: []LogEntry{{Term: 4}, {Term: 4}}},
			reply:    AppendEntriesReply{Term: 5, Success: true},
			wantNext: 7, wantMatch: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := &Raft{
				peers: make([]*labrpc.ClientEnd, 3),
				state: leader, currentTerm: 5, votedFor: 0,
				log:        []LogEntry{{Term: 0}, {Term: 1}, {Term: 1}, {Term: 2}, {Term: 2}, {Term: 4}, {Term: 4}},
				nextIndex:  []int{7, tt.next, 7},
				matchIndex: []int{6, tt.match, 0},
			}
			retry := rf.handleAppendReply(1, &tt.args, &tt.reply)
			if retry != tt.wantRetry || rf.nextIndex[1] != tt.wantNext || rf.matchIndex[1] != tt.wantMatch {
				t.Fatalf("reply produced retry %t, next %d, match %d; want %t, %d, %d",
					retry, rf.nextIndex[1], rf.matchIndex[1], tt.wantRetry, tt.wantNext, tt.wantMatch)
			}
			if tt.reply.Term > 5 {
				if rf.currentTerm != tt.reply.Term || rf.state != follower || rf.votedFor != -1 {
					t.Fatal("higher term reply did not clear leadership and vote")
				}
			} else if rf.currentTerm != 5 || rf.state != leader {
				t.Fatal("reply unexpectedly changed leadership")
			}
		})
	}
}
