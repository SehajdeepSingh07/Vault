package faults

import (
	"sort"
	"strings"
	"sync"
)

type Snapshot struct {
	Enabled bool     `json:"enabled"`
	Blocked []string `json:"blocked_peers,omitempty"`
}

type Injector struct {
	mu      sync.RWMutex
	enabled bool
	blocked map[string]struct{}
}

func New(enabled bool) *Injector {
	return &Injector{enabled: enabled, blocked: make(map[string]struct{})}
}

func (i *Injector) Enabled() bool {
	if i == nil {
		return false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.enabled
}

func (i *Injector) SetEnabled(enabled bool) {
	if i == nil {
		return
	}
	i.mu.Lock()
	i.enabled = enabled
	i.mu.Unlock()
}

func (i *Injector) Block(peerID string) bool {
	if i == nil || !i.Enabled() {
		return false
	}
	peerID = strings.TrimSpace(peerID)
	if peerID == "" {
		return false
	}
	i.mu.Lock()
	i.blocked[peerID] = struct{}{}
	i.mu.Unlock()
	return true
}

func (i *Injector) Unblock(peerID string) {
	if i == nil {
		return
	}
	i.mu.Lock()
	delete(i.blocked, strings.TrimSpace(peerID))
	i.mu.Unlock()
}

func (i *Injector) Clear() {
	if i == nil {
		return
	}
	i.mu.Lock()
	i.blocked = make(map[string]struct{})
	i.mu.Unlock()
}

func (i *Injector) IsBlocked(peerID string) bool {
	if i == nil {
		return false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	if !i.enabled {
		return false
	}
	_, ok := i.blocked[strings.TrimSpace(peerID)]
	return ok
}

func (i *Injector) Snapshot() Snapshot {
	if i == nil {
		return Snapshot{}
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := Snapshot{Enabled: i.enabled, Blocked: make([]string, 0, len(i.blocked))}
	for id := range i.blocked {
		out.Blocked = append(out.Blocked, id)
	}
	sort.Strings(out.Blocked)
	return out
}
