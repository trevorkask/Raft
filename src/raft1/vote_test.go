package raft

import (
	"sync"
	"testing"
	"time"
)

func TestRequestVoteRules(t *testing.T) {
	tests := []struct {
		name         string
		state        role
		votedFor     int
		args         RequestVoteArgs
		wantVote     bool
		wantTerm     int
		wantVotedFor int
		wantState    role
	}{
		{"older term", follower, -1, RequestVoteArgs{2, 1, 2, 2}, false, 3, -1, follower},
		{"first vote", follower, -1, RequestVoteArgs{3, 1, 2, 2}, true, 3, 1, follower},
		{"repeat vote", follower, 1, RequestVoteArgs{3, 1, 2, 2}, true, 3, 1, follower},
		{"different candidate", follower, 1, RequestVoteArgs{3, 2, 2, 2}, false, 3, 1, follower},
		{"candidate already voted for itself", candidate, 0, RequestVoteArgs{3, 1, 2, 2}, false, 3, 0, candidate},
		{"shorter log", follower, -1, RequestVoteArgs{3, 1, 1, 2}, false, 3, -1, follower},
		{"older last log term", follower, -1, RequestVoteArgs{3, 1, 5, 1}, false, 3, -1, follower},
		{"newer last log term", follower, -1, RequestVoteArgs{3, 1, 1, 3}, true, 3, 1, follower},
		{"longer log", follower, -1, RequestVoteArgs{3, 1, 3, 2}, true, 3, 1, follower},
		{"new term clears vote", follower, 2, RequestVoteArgs{4, 1, 2, 2}, true, 4, 1, follower},
		{"leader steps down", leader, 0, RequestVoteArgs{4, 1, 2, 2}, true, 4, 1, follower},
		{"candidate steps down", candidate, 0, RequestVoteArgs{4, 1, 2, 2}, true, 4, 1, follower},
		{"new term with stale log", leader, 0, RequestVoteArgs{4, 1, 1, 1}, false, 4, -1, follower},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deadline := time.Now().Add(-time.Second)
			rf := &Raft{
				state:            tt.state,
				currentTerm:      3,
				votedFor:         tt.votedFor,
				log:              []LogEntry{{Term: 0}, {Term: 1}, {Term: 2}},
				electionDeadline: deadline,
			}
			reply := RequestVoteReply{VoteGranted: true}
			before := time.Now()
			rf.RequestVote(&tt.args, &reply)
			after := time.Now()
			if reply.Term != tt.wantTerm || reply.VoteGranted != tt.wantVote {
				t.Fatalf("reply = %+v, want term %d and vote %t", reply, tt.wantTerm, tt.wantVote)
			}
			if rf.currentTerm != tt.wantTerm || rf.votedFor != tt.wantVotedFor || rf.state != tt.wantState {
				t.Fatalf("state = (%d, %d, %d), want (%d, %d, %d)",
					rf.currentTerm, rf.votedFor, rf.state, tt.wantTerm, tt.wantVotedFor, tt.wantState)
			}
			if tt.wantVote {
				if rf.electionDeadline.Before(before.Add(450*time.Millisecond)) ||
					rf.electionDeadline.After(after.Add(750*time.Millisecond)) {
					t.Fatal("granted vote did not reset the election deadline")
				}
			} else if !rf.electionDeadline.Equal(deadline) {
				t.Fatal("rejected vote changed the election deadline")
			}
		})
	}
}

func TestRequestVoteConcurrent(t *testing.T) {
	rf := &Raft{state: follower, votedFor: -1, log: []LogEntry{{Term: 0}}}
	const candidates = 20
	replies := make([]RequestVoteReply, candidates)
	var wg sync.WaitGroup
	for i := range replies {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			args := RequestVoteArgs{Term: 1, CandidateId: id}
			rf.RequestVote(&args, &replies[id])
		}(i)
	}
	wg.Wait()
	votes := 0
	for id, reply := range replies {
		if reply.Term != 1 {
			t.Fatalf("candidate %d got term %d, want 1", id, reply.Term)
		}
		if reply.VoteGranted {
			votes++
			if rf.votedFor != id {
				t.Fatalf("granted vote to %d but recorded %d", id, rf.votedFor)
			}
		}
	}
	if votes != 1 {
		t.Fatalf("granted %d votes in one term, want 1", votes)
	}
}
