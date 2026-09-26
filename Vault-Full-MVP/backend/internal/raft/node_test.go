package raft

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type testFSM struct {
	mu      sync.Mutex
	items   map[uint64][]byte
	applied []uint64
}

type testRunning struct {
	node   *Node
	server *httptest.Server
	fsm    *testFSM
}

func newTestFSM() *testFSM {
	return &testFSM{items: make(map[uint64][]byte)}
}

func (f *testFSM) Apply(entry LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[entry.Index] = append([]byte(nil), entry.Data...)
	f.applied = append(f.applied, entry.Index)
	return nil
}

func TestThreeNodeElectionAndReplication(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	servers := make(map[string]*httptest.Server)
	runs := make(map[string]testRunning)

	for _, id := range ids {
		servers[id] = httptest.NewUnstartedServer(nil)
	}
	for _, id := range ids {
		server := servers[id]
		fsm := newTestFSM()
		peerList := make([]Peer, 0, 2)
		for _, other := range ids {
			if other != id {
				peerList = append(peerList, Peer{ID: other, Address: "http://" + servers[other].Listener.Addr().String()})
			}
		}
		addr := "http://" + server.Listener.Addr().String()
		node, err := New(Config{
			ID:          id,
			Address:     addr,
			Peers:       peerList,
			DataDir:     t.TempDir(),
			ElectionMin: 180 * time.Millisecond,
			ElectionMax: 320 * time.Millisecond,
			Heartbeat:   60 * time.Millisecond,
			RPCTimeout:  120 * time.Millisecond,
			RandomSeed:  int64([]rune(id)[1]) + 100,
			FSM:         fsm,
		})
		if err != nil {
			t.Fatal(err)
		}
		runs[id] = testRunning{node: node, server: server, fsm: fsm}
	}

	for id, r := range runs {
		id := id
		mux := http.NewServeMux()
		mux.HandleFunc("/internal/v1/raft/request-vote", func(w http.ResponseWriter, req *http.Request) {
			var in RequestVoteRequest
			if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(runs[id].node.ReceiveRequestVote(in))
		})
		mux.HandleFunc("/internal/v1/raft/append-entries", func(w http.ResponseWriter, req *http.Request) {
			var in AppendEntriesRequest
			if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(runs[id].node.ReceiveAppendEntries(in))
		})
		r.server.Config.Handler = mux
		r.server.Start()
	}

	defer func() {
		for _, r := range runs {
			r.node.Stop()
			r.server.Close()
		}
	}()
	for _, r := range runs {
		if err := r.node.Start(); err != nil {
			t.Fatal(err)
		}
	}

	leader := waitLeader(t, runs, 4*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	idx, err := leader.Propose(ctx, []byte(`{"op":"set","key":"alpha"}`))
	if err != nil {
		t.Fatal(err)
	}
	if idx != 1 {
		t.Fatalf("first committed index=%d want=1", idx)
	}

	waitFor(t, 3*time.Second, func() bool {
		for _, r := range runs {
			r.fsm.mu.Lock()
			_, ok := r.fsm.items[idx]
			r.fsm.mu.Unlock()
			if !ok {
				return false
			}
		}
		return true
	})
}

func TestSingleNodeRestartReplaysCommittedMetadataLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "raft")
	fsm := newTestFSM()
	node, err := New(Config{
		ID:          "solo",
		Address:     "http://127.0.0.1:1",
		DataDir:     dir,
		ElectionMin: 80 * time.Millisecond,
		ElectionMax: 120 * time.Millisecond,
		Heartbeat:   30 * time.Millisecond,
		RPCTimeout:  50 * time.Millisecond,
		FSM:         fsm,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return node.Status().Role == RoleLeader })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := node.Propose(ctx, []byte(`{"op":"set","key":"persist"}`)); err != nil {
		t.Fatal(err)
	}
	node.Stop()

	fsm2 := newTestFSM()
	node2, err := New(Config{
		ID:          "solo",
		Address:     "http://127.0.0.1:1",
		DataDir:     dir,
		ElectionMin: 80 * time.Millisecond,
		ElectionMax: 120 * time.Millisecond,
		Heartbeat:   30 * time.Millisecond,
		RPCTimeout:  50 * time.Millisecond,
		FSM:         fsm2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Stop()
	fsm2.mu.Lock()
	if len(fsm2.applied) != 1 || fsm2.applied[0] != 1 {
		fsm2.mu.Unlock()
		t.Fatalf("replayed indexes=%v", fsm2.applied)
	}
	fsm2.mu.Unlock()
	if got := node2.Status().LastApplied; got != 1 {
		t.Fatalf("lastApplied after replay=%d want=1", got)
	}
}

func waitLeader(t *testing.T, nodes map[string]testRunning, timeout time.Duration) *Node {
	t.Helper()
	var leader *Node
	waitFor(t, timeout, func() bool {
		count := 0
		for _, r := range nodes {
			if r.node.Status().Role == RoleLeader {
				leader = r.node
				count++
			}
		}
		return count == 1
	})
	return leader
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition timeout")
}
