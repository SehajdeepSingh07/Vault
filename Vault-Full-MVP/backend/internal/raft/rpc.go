package raft

import "encoding/json"

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term       uint64 `json:"term"`
	Success    bool   `json:"success"`
	MatchIndex uint64 `json:"match_index"`
	ConflictAt uint64 `json:"conflict_at,omitempty"`
}

type persistedState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
	CommitIndex uint64     `json:"commit_index"`
}

type Status struct {
	ID            string `json:"id"`
	Role          Role   `json:"role"`
	CurrentTerm   uint64 `json:"current_term"`
	LeaderID      string `json:"leader_id"`
	CommitIndex   uint64 `json:"commit_index"`
	LastApplied   uint64 `json:"last_applied"`
	LastLogIndex  uint64 `json:"last_log_index"`
	LastLogTerm   uint64 `json:"last_log_term"`
	Quorum        int    `json:"quorum"`
	ClusterSize   int    `json:"cluster_size"`
	ElectionReset string `json:"election_deadline"`
}

var _ = json.RawMessage{}
