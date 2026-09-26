package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/nitesh/vault/internal/raft"
)

func testSHA(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func TestApplyPutAndDeleteVersionHistory(t *testing.T) {
	store := NewStore()
	r1 := Record{Key: "a", Version: 1, Size: 4, SHA256: testSHA("data"), ContentType: "text/plain", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	data1, _ := jsonMarshal(Mutation{Op: "put", Record: r1})
	if err := store.Apply(raft.LogEntry{Index: 1, Term: 1, Type: raft.CommandEntry, Data: data1}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get("a")
	if err != nil || got.Version != 1 {
		t.Fatalf("get version=%+v err=%v", got, err)
	}
	data2, _ := jsonMarshal(Mutation{Op: "delete", Key: "a"})
	if err := store.Apply(raft.LogEntry{Index: 2, Term: 1, Type: raft.CommandEntry, Data: data2}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get("a"); err != ErrNotFound {
		t.Fatalf("get after delete err=%v", err)
	}
	r3 := r1
	r3.Version = 3
	r3.Deleted = false
	r3.UpdatedAt = time.Now().UTC()
	data3, _ := jsonMarshal(Mutation{Op: "put", Record: r3})
	if err := store.Apply(raft.LogEntry{Index: 3, Term: 2, Type: raft.CommandEntry, Data: data3}); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get("a")
	if err != nil || got.Version != 3 {
		t.Fatalf("resurrect version=%d err=%v", got.Version, err)
	}
}

func TestServiceSingleNodePutDelete(t *testing.T) {
	store := NewStore()
	r, err := raft.New(raft.Config{ID: "m1", Address: "http://127.0.0.1:1", DataDir: t.TempDir(), ElectionMin: 60 * time.Millisecond, ElectionMax: 90 * time.Millisecond, Heartbeat: 20 * time.Millisecond, RPCTimeout: 50 * time.Millisecond, FSM: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	svc := NewService(store, r)
	waitMetadataLeader(t, r)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	record, index, err := svc.Put(ctx, "a", 10, testSHA("1234567890"), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 || record.Version != 1 {
		t.Fatalf("record=%+v index=%d", record, index)
	}
	if _, err := svc.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.Put(ctx, "a", 11, testSHA("12345678901"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	got, err := svc.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 3 {
		t.Fatalf("version after delete/recreate=%d", got.Version)
	}
}

func TestValidateRecord(t *testing.T) {
	if err := ValidateRecord(Record{Key: "a", Version: 1, Size: 1, SHA256: testSHA("x")}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRecord(Record{Key: "a", Version: 1, Size: 1, SHA256: "bad"}); err != ErrInvalid {
		t.Fatalf("expected invalid, got %v", err)
	}
}

func waitMetadataLeader(t *testing.T, r *raft.Node) {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.Status().Role == raft.RoleLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("metadata leader election timeout")
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func TestServiceSerializesConcurrentPuts(t *testing.T) {
	store := NewStore()
	r, err := raft.New(raft.Config{ID: "m2", Address: "http://127.0.0.1:1", DataDir: t.TempDir(), ElectionMin: 60 * time.Millisecond, ElectionMax: 90 * time.Millisecond, Heartbeat: 20 * time.Millisecond, RPCTimeout: 50 * time.Millisecond, FSM: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	waitMetadataLeader(t, r)
	svc := NewService(store, r)
	const count = 12
	var wg sync.WaitGroup
	errs := make(chan error, count)
	versions := make(chan uint64, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			record, _, err := svc.Put(ctx, "same", 1, testSHA("x"), "text/plain")
			if err != nil {
				errs <- err
				return
			}
			versions <- record.Version
		}()
	}
	wg.Wait()
	close(errs)
	close(versions)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool)
	for version := range versions {
		seen[version] = true
	}
	if len(seen) != count {
		t.Fatalf("unique committed versions=%d want=%d", len(seen), count)
	}
	for i := uint64(1); i <= count; i++ {
		if !seen[i] {
			t.Fatalf("missing version %d", i)
		}
	}
}

func TestApplyReplicaRepairKeepsVersion(t *testing.T) {
	store := NewStore()
	record := Record{Key: "a", Version: 1, Size: 3, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ContentType: "text/plain", Replicas: []string{"node-1", "node-2"}}
	data, _ := json.Marshal(Mutation{Op: "put", Record: record})
	if err := store.Apply(raft.LogEntry{Type: raft.CommandEntry, Data: data}); err != nil {
		t.Fatal(err)
	}
	repairPayload, _ := json.Marshal(struct {
		Op       string   `json:"op"`
		Key      string   `json:"key"`
		Version  uint64   `json:"version"`
		Replicas []string `json:"replicas"`
	}{Op: "repair_replicas", Key: "a", Version: 1, Replicas: []string{"node-1", "node-3"}})
	if err := store.Apply(raft.LogEntry{Type: raft.CommandEntry, Data: repairPayload}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || len(got.Replicas) != 2 || got.Replicas[1] != "node-3" {
		t.Fatalf("unexpected repaired record: %+v", got)
	}
}
