package cluster

import "time"

type NodeStatus string

const (
	StatusHealthy    NodeStatus = "healthy"
	StatusSuspect    NodeStatus = "suspect"
	StatusFailed     NodeStatus = "failed"
	StatusRecovering NodeStatus = "recovering"
	StatusUnknown    NodeStatus = "unknown"
)

type NodeInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type HeartbeatRequest struct {
	From     NodeInfo  `json:"from"`
	SentAt   time.Time `json:"sent_at"`
	Sequence uint64    `json:"sequence"`
	Protocol int       `json:"protocol"`
}

type PeerView struct {
	NodeInfo
	Status             NodeStatus `json:"status"`
	FailureEpoch       uint64     `json:"failure_epoch"`
	StateSince         time.Time  `json:"state_since,omitempty"`
	LastHeartbeat      time.Time  `json:"last_heartbeat,omitempty"`
	LastHeartbeatAgeMs int64      `json:"last_heartbeat_age_ms,omitempty"`
	MissedHeartbeats   uint64     `json:"missed_heartbeats"`
	HealthyHeartbeats  uint64     `json:"healthy_heartbeats"`
}

type Snapshot struct {
	Self              PeerView   `json:"self"`
	Peers             []PeerView `json:"peers"`
	GeneratedAt       time.Time  `json:"generated_at"`
	TotalNodes        int        `json:"total_nodes"`
	HealthyNodes      int        `json:"healthy_nodes"`
	SuspectNodes      int        `json:"suspect_nodes"`
	RecoveringNodes   int        `json:"recovering_nodes"`
	FailedNodes       int        `json:"failed_nodes"`
	UnknownNodes      int        `json:"unknown_nodes"`
	ReplicationFactor int        `json:"replication_factor,omitempty"`
	WriteQuorum       int        `json:"write_quorum,omitempty"`
	ReadQuorum        int        `json:"read_quorum,omitempty"`
	QuorumAvailable   bool       `json:"quorum_available"`
	DurabilityHealthy bool       `json:"durability_healthy"`
	Degraded          bool       `json:"degraded"`
}
