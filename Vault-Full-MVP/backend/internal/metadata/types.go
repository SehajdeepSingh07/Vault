package metadata

import "time"

type Record struct {
	Key         string    `json:"key"`
	Version     uint64    `json:"version"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	ContentType string    `json:"content_type"`
	Replicas    []string  `json:"replicas,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Deleted     bool      `json:"deleted"`
}

type Mutation struct {
	Op     string `json:"op"`
	Record Record `json:"record"`
	Key    string `json:"key"`
}

// ReplicaRepairMutation replaces the replica membership of an existing
// object version without changing the object version itself.
type ReplicaRepairMutation struct {
	Key      string   `json:"key"`
	Version  uint64   `json:"version"`
	Replicas []string `json:"replicas"`
}
