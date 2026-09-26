package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nitesh/vault/internal/raft"
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// Service is the client-facing metadata layer backed by the Raft state machine.
type Service struct {
	store *Store
	raft  *raft.Node
	mu    sync.Mutex
}

func NewService(store *Store, r *raft.Node) *Service {
	return &Service{store: store, raft: r}
}

func (s *Service) Store() *Store    { return s.store }
func (s *Service) Raft() *raft.Node { return s.raft }

func (s *Service) Put(ctx context.Context, key string, size int64, sha256, contentType string) (Record, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "" {
		return Record{}, 0, errors.New("metadata key cannot be empty")
	}
	previous, ok := s.store.GetRaw(key)
	version := uint64(1)
	created := time.Now().UTC()
	if ok {
		version = previous.Version + 1
		created = previous.CreatedAt
	}
	record := Record{
		Key:         key,
		Version:     version,
		Size:        size,
		SHA256:      strings.ToLower(sha256),
		ContentType: contentType,
		CreatedAt:   created,
		UpdatedAt:   time.Now().UTC(),
	}
	return s.putRecordLocked(ctx, record)
}

// PutRecord commits an explicitly versioned record. The version must be exactly
// one greater than the current metadata version (or 1 for a new key). This is
// used by the distributed data plane so bytes can be staged before metadata is
// committed, without allowing concurrent writes to reuse the same version.
func (s *Service) PutRecord(ctx context.Context, record Record) (Record, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putRecordLocked(ctx, record)
}

func (s *Service) putRecordLocked(ctx context.Context, record Record) (Record, uint64, error) {
	if record.Key == "" {
		return Record{}, 0, errors.New("metadata key cannot be empty")
	}
	if record.Size < 0 {
		return Record{}, 0, errors.New("metadata size cannot be negative")
	}
	if !sha256Pattern.MatchString(record.SHA256) {
		return Record{}, 0, errors.New("metadata sha256 must be 64 hexadecimal characters")
	}
	if record.ContentType == "" {
		record.ContentType = "application/octet-stream"
	}
	record.SHA256 = strings.ToLower(record.SHA256)
	record.Replicas = uniqueSortedReplicas(record.Replicas)
	if record.Version == 0 {
		return Record{}, 0, errors.New("metadata version must be positive")
	}
	previous, ok := s.store.GetRaw(record.Key)
	expectedVersion := uint64(1)
	created := record.CreatedAt
	if ok {
		expectedVersion = previous.Version + 1
		if created.IsZero() {
			created = previous.CreatedAt
		}
	}
	if record.Version != expectedVersion {
		return Record{}, 0, fmt.Errorf("metadata version conflict: got %d want %d", record.Version, expectedVersion)
	}
	if created.IsZero() {
		created = time.Now().UTC()
	}
	record.CreatedAt = created
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = time.Now().UTC()
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, 0, err
	}
	mutation, err := json.Marshal(Mutation{Op: "put", Record: record})
	if err != nil {
		return Record{}, 0, fmt.Errorf("encode metadata mutation: %w", err)
	}
	index, err := s.raft.Propose(ctx, mutation)
	if err != nil {
		return Record{}, 0, err
	}
	committed, err := s.store.Get(record.Key)
	if err != nil {
		return Record{}, index, fmt.Errorf("metadata committed at index %d but readback failed: %w", index, err)
	}
	return committed, index, nil
}

func (s *Service) Delete(ctx context.Context, key string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "" {
		return 0, errors.New("metadata key cannot be empty")
	}
	mutation, _ := json.Marshal(Mutation{Op: "delete", Key: key})
	index, err := s.raft.Propose(ctx, mutation)
	if err != nil {
		return 0, err
	}
	return index, nil
}

func (s *Service) Get(key string) (Record, error) {
	return s.store.Get(key)
}

func (s *Service) Raw(key string) (Record, bool) {
	return s.store.GetRaw(key)
}

func (s *Service) ListActive() []Record {
	return s.store.ListActive()
}

func uniqueSortedReplicas(replicas []string) []string {
	if len(replicas) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(replicas))
	out := make([]string, 0, len(replicas))
	for _, id := range replicas {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// UpdateReplicas commits a replica-set change for an existing object version.
// The object version does not change; only the durable replica membership does.
func (s *Service) UpdateReplicas(ctx context.Context, key string, version uint64, replicas []string) (Record, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "" || version == 0 {
		return Record{}, 0, errors.New("repair key and version are required")
	}
	current, ok := s.store.GetRaw(key)
	if !ok || current.Deleted {
		return Record{}, 0, ErrNotFound
	}
	if current.Version != version {
		return Record{}, 0, fmt.Errorf("metadata repair version conflict: got %d want %d", version, current.Version)
	}
	mutation, err := json.Marshal(struct {
		Op       string   `json:"op"`
		Key      string   `json:"key"`
		Version  uint64   `json:"version"`
		Replicas []string `json:"replicas"`
	}{Op: "repair_replicas", Key: key, Version: version, Replicas: uniqueSortedReplicas(replicas)})
	if err != nil {
		return Record{}, 0, fmt.Errorf("encode replica repair mutation: %w", err)
	}
	index, err := s.raft.Propose(ctx, mutation)
	if err != nil {
		return Record{}, 0, err
	}
	updated, err := s.store.Get(key)
	if err != nil {
		return Record{}, index, err
	}
	return updated, index, nil
}
