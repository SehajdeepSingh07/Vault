package raft

import "encoding/json"

type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

type Peer struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type LogEntry struct {
	Index uint64          `json:"index"`
	Term  uint64          `json:"term"`
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data"`
}
