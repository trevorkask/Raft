package raft

import (
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/tester1"
)

func TestHeartbeatTerms(t *testing.T) {
	tests := []struct {
		name        string
		term        int
		wantTerm    int
		wantState   role
		wantVote    int
		wantSuccess bool
	}{
		{"old leader", 2, 3, candidate, 0, false},
		{"current leader", 3, 3, follower, 0, true},
		{"new leader", 4, 4, follower, -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deadline := time.Now().Add(-time.Second)
			rf := &Raft{persister: tester.MakePersister(), currentTerm: 3, state: candidate, votedFor: 0, electionDeadline: deadline, log: []LogEntry{{Term: 0}}}
			args := AppendEntriesArgs{Term: tt.term, LeaderId: 1}
			reply := AppendEntriesReply{Success: true}
			rf.AppendEntries(&args, &reply)
			if reply.Term != tt.wantTerm || reply.Success != tt.wantSuccess {
				t.Fatalf("reply = %+v, want term %d and success %t", reply, tt.wantTerm, tt.wantSuccess)
			}
			if rf.currentTerm != tt.wantTerm || rf.state != tt.wantState || rf.votedFor != tt.wantVote {
				t.Fatalf("state = (%d, %d, %d), want (%d, %d, %d)",
					rf.currentTerm, rf.state, rf.votedFor, tt.wantTerm, tt.wantState, tt.wantVote)
			}
			if tt.wantSuccess {
				if !rf.electionDeadline.After(deadline) {
					t.Fatal("heartbeat did not reset the election deadline")
				}
			} else if !rf.electionDeadline.Equal(deadline) {
				t.Fatal("stale heartbeat changed the election deadline")
			}
		})
	}
}

func TestSinglePeerElection(t *testing.T) {
	rf := &Raft{
		persister: tester.MakePersister(),
		peers:     make([]*labrpc.ClientEnd, 1),
		state:     follower,
		votedFor:  -1,
		log:       []LogEntry{{Term: 0}},
	}
	rf.mu.Lock()
	rf.startElection()
	rf.mu.Unlock()
	term, isLeader := rf.GetState()
	if term != 1 || !isLeader || rf.votedFor != rf.me {
		t.Fatalf("single peer did not elect itself: term %d, leader %t, vote %d", term, isLeader, rf.votedFor)
	}
}
