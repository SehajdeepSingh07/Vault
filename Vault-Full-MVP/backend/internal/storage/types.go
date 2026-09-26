package storage

import "time"

type Metadata struct {
	Key         string    `json:"key"`
	Version     uint64    `json:"version"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	ContentType string    `json:"content_type"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
