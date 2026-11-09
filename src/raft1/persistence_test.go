package raft

import (
	"reflect"
	"testing"

	"6.5840/labrpc"
	"6.5840/tester1"
)

func TestPersistentChanges(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Raft)
	}{
		{"election", func(rf *Raft) { rf.startElection() }},
		{"granted vote", func(rf *Raft) {
			rf.RequestVote(&RequestVoteArgs{Term: 4, CandidateId: 1, LastLogIndex: 2, LastLogTerm: 2}, &RequestVoteReply{})
		}},
		{"new term with rejected vote", func(rf *Raft) {
			rf.RequestVote(&RequestVoteArgs{Term: 4, CandidateId: 1}, &RequestVoteReply{})
		}},
		{"appended entries", func(rf *Raft) {
			rf.AppendEntries(&AppendEntriesArgs{Term: 3, PrevLogIndex: 2, PrevLogTerm: 2, Entries: []LogEntry{{Term: 3, Command: "c"}}}, &AppendEntriesReply{})
		}},
		{"replaced suffix", func(rf *Raft) {
			rf.AppendEntries(&AppendEntriesArgs{Term: 3, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []LogEntry{{Term: 3, Command: "c"}}}, &AppendEntriesReply{})
		}},
		{"new term with rejected append", func(rf *Raft) {
			rf.AppendEntries(&AppendEntriesArgs{Term: 4, PrevLogIndex: 10, PrevLogTerm: 2}, &AppendEntriesReply{})
		}},
		{"leader command", func(rf *Raft) {
			rf.becomeLeader()
			rf.Start("c")
		}},
		{"higher term reply", func(rf *Raft) {
			rf.becomeLeader()
			rf.handleAppendReply(0, &AppendEntriesArgs{Term: 3}, &AppendEntriesReply{Term: 4})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := &Raft{
				peers:       make([]*labrpc.ClientEnd, 1),
				persister:   tester.MakePersister(),
				currentTerm: 3,
				votedFor:    0,
				log:         []LogEntry{{Term: 0}, {Term: 1, Command: "a"}, {Term: 2, Command: 42}},
			}
			rf.persist()
			tt.change(rf)
			restarted := &Raft{persister: rf.persister.Checkpoint(), votedFor: -1, log: []LogEntry{{Term: 0}}}
			restarted.readPersist(restarted.persister.ReadRaftState())
			if restarted.currentTerm != rf.currentTerm || restarted.votedFor != rf.votedFor || !reflect.DeepEqual(restarted.log, rf.log) {
				t.Fatalf("restored term %d, vote %d, log %+v; want %d, %d, %+v",
					restarted.currentTerm, restarted.votedFor, restarted.log, rf.currentTerm, rf.votedFor, rf.log)
			}
			if restarted.state != follower || restarted.commitIndex != 0 || restarted.lastApplied != 0 {
				t.Fatal("restart restored volatile state")
			}
		})
	}
}

func TestVoteSurvivesRestart(t *testing.T) {
	rf := &Raft{persister: tester.MakePersister(), votedFor: -1, log: []LogEntry{{Term: 0}}}
	reply := RequestVoteReply{}
	rf.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 1}, &reply)
	if !reply.VoteGranted {
		t.Fatal("first vote was rejected")
	}
	restarted := &Raft{persister: rf.persister.Checkpoint(), votedFor: -1, log: []LogEntry{{Term: 0}}}
	restarted.readPersist(restarted.persister.ReadRaftState())
	reply = RequestVoteReply{}
	restarted.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 2}, &reply)
	if reply.VoteGranted || reply.Term != 1 {
		t.Fatal("restart allowed a second vote in the same term")
	}
	restarted.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 1}, &reply)
	if !reply.VoteGranted {
		t.Fatal("restart forgot the original candidate")
	}
	restarted.RequestVote(&RequestVoteArgs{Term: 2, CandidateId: 2}, &reply)
	if !reply.VoteGranted || reply.Term != 2 {
		t.Fatal("restart prevented voting in a newer term")
	}
}
