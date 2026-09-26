package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const stateFile = "raft-state.json"

func loadState(dir string) (persistedState, error) {
	path := filepath.Join(dir, stateFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return persistedState{Log: []LogEntry{{Index: 0, Term: 0}}}, nil
		}
		return persistedState{}, fmt.Errorf("read raft state: %w", err)
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return persistedState{}, fmt.Errorf("decode raft state: %w", err)
	}
	if len(st.Log) == 0 || st.Log[0].Index != 0 {
		return persistedState{}, errors.New("raft log sentinel missing")
	}
	return st, nil
}

func persistState(dir string, st persistedState) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create raft state directory: %w", err)
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".raft-state-*")
	if err != nil {
		return fmt.Errorf("create raft state temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod raft state: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync raft state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close raft state: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, stateFile)); err != nil {
		return fmt.Errorf("commit raft state: %w", err)
	}
	return nil
}
