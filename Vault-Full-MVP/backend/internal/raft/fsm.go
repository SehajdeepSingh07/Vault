package raft

import "encoding/json"

type FSM interface {
	Apply(entry LogEntry) error
}

// Command is the generic metadata state-machine command carried by the raft log.
type Command struct {
	Op   string          `json:"op"`
	Data json.RawMessage `json:"data"`
}
