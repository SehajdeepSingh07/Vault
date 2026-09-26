package cluster

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHeartbeatMarksPeerHealthy(t *testing.T) {
	m, err := New(Config{
		Self:              NodeInfo{ID: "node-1", Address: "http://127.0.0.1:10001"},
		Peers:             []NodeInfo{{ID: "node-2", Address: "http://127.0.0.1:10002"}},
		HeartbeatInterval: 20 * time.Millisecond,
		SuspectAfter:      60 * time.Millisecond,
		FailAfter:         120 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = m.ReceiveHeartbeat(HeartbeatRequest{From: NodeInfo{ID: "node-2", Address: "http://127.0.0.1:10002"}, Protocol: protocolVersion, SentAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	status, ok := m.Status("node-2")
	if !ok || status != StatusHealthy {
		t.Fatalf("status=%q ok=%v", status, ok)
	}
}

func TestUnknownHeartbeatRejected(t *testing.T) {
	m, err := New(Config{Self: NodeInfo{ID: "node-1", Address: "http://127.0.0.1:10001"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ReceiveHeartbeat(HeartbeatRequest{From: NodeInfo{ID: "evil", Address: "http://127.0.0.1:9"}, Protocol: protocolVersion}); err == nil {
		t.Fatal("expected unknown heartbeat to be rejected")
	}
}

func TestFailureStateTransition(t *testing.T) {
	m, err := New(Config{
		Self:              NodeInfo{ID: "node-1", Address: "http://127.0.0.1:10001"},
		Peers:             []NodeInfo{{ID: "node-2", Address: "http://127.0.0.1:10002"}},
		HeartbeatInterval: 10 * time.Millisecond,
		SuspectAfter:      20 * time.Millisecond,
		FailAfter:         40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.peers["node-2"].lastHeartbeat = time.Now().UTC().Add(-100 * time.Millisecond)
	m.mu.Unlock()
	m.advanceFailures()
	status, _ := m.Status("node-2")
	if status != StatusFailed {
		t.Fatalf("status=%q", status)
	}
}

func TestHeartbeatHTTPRoundTrip(t *testing.T) {
	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/internal/v1/heartbeat" {
			select {
			case received <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	m, err := New(Config{
		Self:              NodeInfo{ID: "node-1", Address: "http://127.0.0.1:10001"},
		Peers:             []NodeInfo{{ID: "node-2", Address: server.URL}},
		HeartbeatInterval: 10 * time.Millisecond,
		HTTPClient:        &http.Client{Timeout: 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	select {
	case <-received:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not send heartbeat to peer")
	}
}

func TestFailureEpisodeEpochAndRecovery(t *testing.T) {
	m, err := New(Config{
		Self:               NodeInfo{ID: "node-1", Address: "http://127.0.0.1:10001"},
		Peers:              []NodeInfo{{ID: "node-2", Address: "http://127.0.0.1:10002"}},
		HeartbeatInterval:  10 * time.Millisecond,
		SuspectAfter:       20 * time.Millisecond,
		FailAfter:          40 * time.Millisecond,
		RecoveryHeartbeats: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.peers["node-2"].lastHeartbeat = time.Now().UTC().Add(-100 * time.Millisecond)
	m.mu.Unlock()
	m.advanceFailures()
	status, ok := m.Status("node-2")
	if !ok || status != StatusFailed {
		t.Fatalf("status=%q ok=%v", status, ok)
	}
	epoch, _ := m.FailureEpoch("node-2")
	if epoch != 1 {
		t.Fatalf("epoch=%d", epoch)
	}
	for i := 0; i < 2; i++ {
		if err := m.ReceiveHeartbeat(HeartbeatRequest{From: NodeInfo{ID: "node-2", Address: "http://127.0.0.1:10002"}, Protocol: protocolVersion, SentAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	status, _ = m.Status("node-2")
	if status != StatusHealthy {
		t.Fatalf("recovered status=%q", status)
	}
	epoch2, _ := m.FailureEpoch("node-2")
	if epoch2 != 1 {
		t.Fatalf("epoch changed during recovery: %d", epoch2)
	}
}

func TestSnapshotShowsDegradedCluster(t *testing.T) {
	m, err := New(Config{
		Self: NodeInfo{ID: "node-1", Address: "http://127.0.0.1:10001"},
		Peers: []NodeInfo{
			{ID: "node-2", Address: "http://127.0.0.1:10002"},
			{ID: "node-3", Address: "http://127.0.0.1:10003"},
		},
		ReplicationFactor: 3,
		WriteQuorum:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.peers["node-2"].status = StatusFailed
	m.peers["node-2"].failureEpoch = 1
	m.peers["node-3"].status = StatusHealthy
	m.mu.Unlock()
	snap := m.Snapshot()
	if snap.HealthyNodes != 2 || snap.FailedNodes != 1 {
		t.Fatalf("bad counts: %+v", snap)
	}
	if snap.QuorumAvailable != true {
		t.Fatal("write quorum should still be available")
	}
	if snap.DurabilityHealthy {
		t.Fatal("RF=3 with two healthy nodes must be degraded")
	}
	if !snap.Degraded {
		t.Fatal("cluster should be degraded")
	}
}
