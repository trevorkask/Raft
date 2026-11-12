package raft_test

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"6.5840/labrpc"
	impl "6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Raft struct {
	mu        sync.Mutex
	behind    bool
	accept    bool
	installed bool
	snapshots chan impl.InstallSnapshotArgs
	appends   chan impl.AppendEntriesArgs
}

func (r *Raft) RequestVote(args *impl.RequestVoteArgs, reply *impl.RequestVoteReply) {
	reply.Term = args.Term
	reply.VoteGranted = true
}

func (r *Raft) AppendEntries(args *impl.AppendEntriesArgs, reply *impl.AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply.Term = args.Term
	if r.behind && !r.installed {
		reply.ConflictTerm = -1
		reply.ConflictIndex = 1
		return
	}
	reply.Success = true
	if r.installed && len(args.Entries) > 0 {
		select {
		case r.appends <- *args:
		default:
		}
	}
}

func (r *Raft) InstallSnapshot(args *impl.InstallSnapshotArgs, reply *impl.InstallSnapshotReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply.Term = args.Term
	reply.Success = r.accept
	if r.accept {
		r.installed = true
	}
	select {
	case r.snapshots <- *args:
	default:
	}
}

func TestSnapshotSender(t *testing.T) {
	network := labrpc.MakeNetwork()
	defer network.Cleanup()
	end := network.MakeEnd("follower")
	receiver := &Raft{behind: true, snapshots: make(chan impl.InstallSnapshotArgs, 10), appends: make(chan impl.AppendEntriesArgs, 10)}
	server := labrpc.MakeServer()
	server.AddService(labrpc.MakeService(receiver))
	network.AddServer(1, server)
	network.Connect("follower", 1)
	network.Enable("follower", true)
	healthyEnd := network.MakeEnd("healthy")
	healthyServer := labrpc.MakeServer()
	healthyServer.AddService(labrpc.MakeService(&Raft{}))
	network.AddServer(2, healthyServer)
	network.Connect("healthy", 2)
	network.Enable("healthy", true)
	applyCh := make(chan raftapi.ApplyMsg, 10)
	rf := impl.Make([]*labrpc.ClientEnd{nil, end, healthyEnd}, 0, tester.MakePersister(), applyCh)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, leader := rf.GetState(); leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader elected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for command := 1; command <= 3; command++ {
		rf.Start(command)
		select {
		case msg := <-applyCh:
			if msg.CommandIndex != command {
				t.Fatal("unexpected applied index")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("command did not commit")
		}
	}
	term, _ := rf.GetState()
	rf.Snapshot(3, []byte("state at 3"))
	rf.Start(4)
	select {
	case args := <-receiver.snapshots:
		if args.Term != term || args.LeaderId != 0 || args.LastIncludedIndex != 3 || args.LastIncludedTerm != term || !bytes.Equal(args.Data, []byte("state at 3")) {
			t.Fatalf("incorrect snapshot request: %+v", args)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lagging follower did not receive a snapshot")
	}
	receiver.mu.Lock()
	receiver.accept = true
	receiver.mu.Unlock()
	select {
	case args := <-receiver.appends:
		if args.PrevLogIndex != 3 || len(args.Entries) != 1 || args.Entries[0].Command != 4 {
			t.Fatalf("replication resumed at the wrong index: %+v", args)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("sender did not retry the snapshot and resume replication")
	}
}
