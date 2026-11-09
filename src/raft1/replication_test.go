package raft

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/tester1"
)

func TestAppendEntriesLogs(t *testing.T) {
	base := []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 2, Command: "b"}, {Term: 2, Command: "c"}}
	tests := []struct {
		name        string
		args        AppendEntriesArgs
		wantSuccess bool
		wantLog     []LogEntry
	}{
		{
			name:    "stale term",
			args:    AppendEntriesArgs{Term: 2, PrevLogIndex: 3, PrevLogTerm: 2, Entries: []LogEntry{{Term: 2, Command: "d"}}},
			wantLog: base,
		},
		{
			name:    "missing previous entry",
			args:    AppendEntriesArgs{Term: 3, PrevLogIndex: 4, PrevLogTerm: 2},
			wantLog: base,
		},
		{
			name:    "previous term mismatch",
			args:    AppendEntriesArgs{Term: 3, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []LogEntry{{Term: 3, Command: "d"}}},
			wantLog: base,
		},
		{
			name:        "heartbeat preserves suffix",
			args:        AppendEntriesArgs{Term: 3, PrevLogIndex: 1, PrevLogTerm: 1},
			wantSuccess: true,
			wantLog:     base,
		},
		{
			name:        "duplicate preserves suffix",
			args:        AppendEntriesArgs{Term: 3, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Term: 2, Command: "b"}}},
			wantSuccess: true,
			wantLog:     base,
		},
		{
			name:        "replace conflicting suffix",
			args:        AppendEntriesArgs{Term: 3, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Term: 3, Command: "d"}}},
			wantSuccess: true,
			wantLog:     []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 3, Command: "d"}},
		},
		{
			name:        "append after matching entries",
			args:        AppendEntriesArgs{Term: 3, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Term: 2, Command: "b"}, {Term: 2, Command: "c"}, {Term: 3, Command: "d"}}},
			wantSuccess: true,
			wantLog:     []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 2, Command: "b"}, {Term: 2, Command: "c"}, {Term: 3, Command: "d"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := &Raft{persister: tester.MakePersister(), currentTerm: 3, votedFor: -1, log: append([]LogEntry(nil), base...)}
			for attempt := 0; attempt < 2; attempt++ {
				reply := AppendEntriesReply{}
				rf.AppendEntries(&tt.args, &reply)
				if reply.Term != 3 || reply.Success != tt.wantSuccess {
					t.Fatalf("reply = %+v, want term 3 and success %t", reply, tt.wantSuccess)
				}
				if !reflect.DeepEqual(rf.log, tt.wantLog) {
					t.Fatalf("log = %+v, want %+v", rf.log, tt.wantLog)
				}
			}
		})
	}
}

func TestStartAppends(t *testing.T) {
	rf := &Raft{persister: tester.MakePersister(), peers: make([]*labrpc.ClientEnd, 1), currentTerm: 3, log: []LogEntry{{Term: 0}}}
	if index, term, ok := rf.Start("rejected"); index != -1 || term != 3 || ok || len(rf.log) != 1 {
		t.Fatal("follower accepted a command")
	}
	rf.becomeLeader()
	const commands = 20
	indices := make([]int, commands)
	var wg sync.WaitGroup
	for command := range indices {
		wg.Add(1)
		go func(command int) {
			defer wg.Done()
			index, term, ok := rf.Start(command)
			if term != 3 || !ok {
				t.Errorf("leader rejected command %d in term %d", command, term)
			}
			indices[command] = index
		}(command)
	}
	wg.Wait()
	seen := make(map[int]bool)
	for command, index := range indices {
		if index < 1 || index > commands || seen[index] {
			t.Fatalf("invalid or duplicate log index %d", index)
		}
		seen[index] = true
		if entry := rf.log[index]; entry.Term != 3 || entry.Command != command {
			t.Fatalf("index %d contains %+v, want command %d in term 3", index, entry, command)
		}
	}
	if len(rf.log) != commands+1 || rf.matchIndex[0] != commands {
		t.Fatal("leader did not record all commands")
	}
}

func TestReplicateFollower(t *testing.T) {
	network := labrpc.MakeNetwork()
	defer network.Cleanup()
	end := network.MakeEnd("leader-follower")
	f := &Raft{persister: tester.MakePersister(), currentTerm: 2, votedFor: -1, log: []LogEntry{{Term: 0}}}
	server := labrpc.MakeServer()
	server.AddService(labrpc.MakeService(f))
	network.AddServer("follower", server)
	network.Connect("leader-follower", "follower")
	network.Enable("leader-follower", true)
	rf := &Raft{
		persister:   tester.MakePersister(),
		peers:       []*labrpc.ClientEnd{nil, end},
		currentTerm: 3,
		log:         []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 2, Command: "b"}},
	}
	rf.becomeLeader()
	for attempt := 0; attempt < 3; attempt++ {
		rf.replicateTo(1)
	}
	f.mu.Lock()
	initialLog := append([]LogEntry(nil), f.log...)
	f.mu.Unlock()
	if !reflect.DeepEqual(initialLog, rf.log) || rf.matchIndex[1] != 2 || rf.nextIndex[1] != 3 {
		t.Fatalf("follower did not catch up: log %+v, match %d, next %d", initialLog, rf.matchIndex[1], rf.nextIndex[1])
	}

	network.Enable("leader-follower", false)
	index, term, ok := rf.Start("c")
	if index != 3 || term != 3 || !ok {
		t.Fatalf("Start = (%d, %d, %t), want (3, 3, true)", index, term, ok)
	}
	rf.replicateTo(1)
	f.mu.Lock()
	disconnectedLength := len(f.log)
	f.mu.Unlock()
	if disconnectedLength != 3 {
		t.Fatal("disconnected follower received an entry")
	}
	network.Enable("leader-follower", true)
	rf.mu.Lock()
	rf.broadcastAppendEntries()
	rf.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for {
		rf.mu.Lock()
		matched := rf.matchIndex[1] == 3
		rf.mu.Unlock()
		if matched {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reconnected follower did not receive the new entry")
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.log) != 4 || f.log[3].Term != 3 || f.log[3].Command != "c" {
		t.Fatalf("follower log = %+v, want command c at index 3 in term 3", f.log)
	}
}
