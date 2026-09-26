package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nitesh/vault/internal/raft"
)

var ErrNotFound = errors.New("metadata not found")
var ErrInvalid = errors.New("invalid metadata")
var ErrDeleted = errors.New("metadata deleted")

type Store struct {
	mu      sync.RWMutex
	records map[string]Record
}

func NewStore() *Store {
	return &Store{records: make(map[string]Record)}
}

func (s *Store) Get(key string) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[key]
	if !ok || record.Deleted {
		return Record{}, ErrNotFound
	}
	return record, nil
}

func (s *Store) GetRaw(key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[key]
	return record, ok
}

func (s *Store) ListActive() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		if record.Deleted {
			continue
		}
		record.Replicas = append([]string(nil), record.Replicas...)
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}

func (s *Store) Apply(entry raft.LogEntry) error {
	if entry.Type != raft.CommandEntry {
		return nil
	}
	var mutation Mutation
	if err := json.Unmarshal(entry.Data, &mutation); err != nil {
		return fmt.Errorf("decode metadata mutation: %w", err)
	}
	if mutation.Op != "put" && mutation.Op != "delete" && mutation.Op != "repair_replicas" {
		return fmt.Errorf("unknown metadata mutation %q", mutation.Op)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if mutation.Op == "repair_replicas" {
		var repair ReplicaRepairMutation
		if err := json.Unmarshal(entry.Data, &repair); err != nil {
			return fmt.Errorf("decode replica repair mutation: %w", err)
		}
		if repair.Key == "" || repair.Version == 0 {
			return ErrInvalid
		}
		previous, ok := s.records[repair.Key]
		if !ok || previous.Deleted || previous.Version != repair.Version {
			return nil
		}
		previous.Replicas = uniqueSortedReplicas(repair.Replicas)
		previous.UpdatedAt = time.Now().UTC()
		s.records[repair.Key] = previous
		return nil
	}
	if mutation.Op == "delete" {
		previous, ok := s.records[mutation.Key]
		version := uint64(1)
		created := time.Now().UTC()
		if ok {
			version = previous.Version + 1
			created = previous.CreatedAt
		}
		replicas := []string(nil)
		if ok {
			replicas = append([]string(nil), previous.Replicas...)
		}
		s.records[mutation.Key] = Record{Key: mutation.Key, Version: version, Size: previous.Size, SHA256: previous.SHA256, ContentType: previous.ContentType, Replicas: replicas, UpdatedAt: time.Now().UTC(), CreatedAt: created, Deleted: true}
		return nil
	}
	if mutation.Record.Key == "" || mutation.Record.Version == 0 || mutation.Record.Size < 0 || len(mutation.Record.SHA256) != 64 {
		return ErrInvalid
	}
	previous, ok := s.records[mutation.Record.Key]
	if ok && mutation.Record.Version <= previous.Version {
		return nil
	}
	mutation.Record.Replicas = uniqueSortedReplicas(mutation.Record.Replicas)
	s.records[mutation.Record.Key] = mutation.Record
	return nil
}

func ValidateRecord(record Record) error {
	if record.Key == "" || record.Version == 0 || record.Size < 0 || len(record.SHA256) != 64 {
		return ErrInvalid
	}
	return nil
}
