package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nitesh/vault/internal/faults"
)

const protocolVersion = 1

type Config struct {
	Self               NodeInfo
	Peers              []NodeInfo
	HeartbeatInterval  time.Duration
	SuspectAfter       time.Duration
	FailAfter          time.Duration
	RecoveryHeartbeats uint64
	ReplicationFactor  int
	WriteQuorum        int
	ReadQuorum         int
	HTTPClient         *http.Client
	Faults             *faults.Injector
}

type peerState struct {
	info              NodeInfo
	status            NodeStatus
	lastHeartbeat     time.Time
	stateSince        time.Time
	missedHeartbeats  uint64
	healthyHeartbeats uint64
	failureEpoch      uint64
	episodeActive     bool
}

type Membership struct {
	self               NodeInfo
	peers              map[string]*peerState
	mu                 sync.RWMutex
	started            bool
	stopOnce           sync.Once
	stopCh             chan struct{}
	wg                 sync.WaitGroup
	interval           time.Duration
	suspectAfter       time.Duration
	failAfter          time.Duration
	recoveryHeartbeats uint64
	replicationFactor  int
	writeQuorum        int
	readQuorum         int
	httpClient         *http.Client
	faults             *faults.Injector
	sequence           uint64
}

func New(cfg Config) (*Membership, error) {
	if strings.TrimSpace(cfg.Self.ID) == "" {
		return nil, errors.New("self node id cannot be empty")
	}
	address := normalizeAddress(cfg.Self.Address)
	if address == "" {
		return nil, errors.New("self node address cannot be empty")
	}
	cfg.Self.Address = address
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = time.Second
	}
	if cfg.SuspectAfter <= cfg.HeartbeatInterval {
		cfg.SuspectAfter = 3 * cfg.HeartbeatInterval
	}
	if cfg.FailAfter <= cfg.SuspectAfter {
		cfg.FailAfter = 3 * cfg.SuspectAfter
	}
	if cfg.RecoveryHeartbeats == 0 {
		cfg.RecoveryHeartbeats = 2
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 800 * time.Millisecond}
	}

	peers := make(map[string]*peerState, len(cfg.Peers))
	for _, info := range cfg.Peers {
		info.Address = normalizeAddress(info.Address)
		if info.ID == "" || info.Address == "" || info.ID == cfg.Self.ID {
			continue
		}
		if _, exists := peers[info.ID]; exists {
			return nil, fmt.Errorf("duplicate peer id %q", info.ID)
		}
		peers[info.ID] = &peerState{info: info, status: StatusUnknown}
	}

	return &Membership{
		self:               cfg.Self,
		peers:              peers,
		stopCh:             make(chan struct{}),
		interval:           cfg.HeartbeatInterval,
		suspectAfter:       cfg.SuspectAfter,
		failAfter:          cfg.FailAfter,
		recoveryHeartbeats: cfg.RecoveryHeartbeats,
		replicationFactor:  cfg.ReplicationFactor,
		writeQuorum:        cfg.WriteQuorum,
		readQuorum:         cfg.ReadQuorum,
		httpClient:         cfg.HTTPClient,
		faults:             cfg.Faults,
	}, nil
}

func (m *Membership) Self() NodeInfo { return m.self }

func (m *Membership) Start() error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return errors.New("membership already started")
	}
	m.started = true
	m.mu.Unlock()

	m.wg.Add(1)
	go m.run()
	return nil
}

func (m *Membership) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
}

func (m *Membership) ReceiveHeartbeat(req HeartbeatRequest) error {
	if req.Protocol != 0 && req.Protocol != protocolVersion {
		return fmt.Errorf("unsupported heartbeat protocol %d", req.Protocol)
	}
	if req.From.ID == "" || req.From.Address == "" {
		return errors.New("heartbeat sender is incomplete")
	}
	normalized := normalizeAddress(req.From.Address)
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[req.From.ID]
	if !ok {
		return fmt.Errorf("unknown peer %q", req.From.ID)
	}
	if peer.info.Address != normalized {
		return fmt.Errorf("peer %q address mismatch", req.From.ID)
	}
	now := time.Now().UTC()
	if !req.SentAt.IsZero() && req.SentAt.After(now.Add(5*time.Second)) {
		return errors.New("heartbeat timestamp is too far in the future")
	}

	wasFailed := peer.status == StatusFailed
	wasSuspect := peer.status == StatusSuspect
	peer.lastHeartbeat = now
	peer.missedHeartbeats = 0
	peer.healthyHeartbeats++

	switch peer.status {
	case StatusHealthy:
		peer.healthyHeartbeats = minUint64(peer.healthyHeartbeats, m.recoveryHeartbeats)
	case StatusRecovering:
		if peer.healthyHeartbeats >= m.recoveryHeartbeats {
			m.transitionLocked(peer, StatusHealthy, now)
			peer.episodeActive = false
		}
	case StatusUnknown:
		m.transitionLocked(peer, StatusHealthy, now)
		peer.episodeActive = false
		peer.failureEpoch = 0
	case StatusFailed, StatusSuspect:
		if !peer.episodeActive && (wasFailed || wasSuspect) {
			peer.failureEpoch++
			peer.episodeActive = true
		}
		m.transitionLocked(peer, StatusRecovering, now)
		if peer.healthyHeartbeats >= m.recoveryHeartbeats {
			m.transitionLocked(peer, StatusHealthy, now)
			peer.episodeActive = false
		}
	}
	return nil
}

func (m *Membership) Snapshot() Snapshot {
	now := time.Now().UTC()
	m.mu.RLock()
	defer m.mu.RUnlock()

	self := PeerView{NodeInfo: m.self, Status: StatusHealthy, StateSince: now, LastHeartbeat: now}
	peers := make([]PeerView, 0, len(m.peers))
	counts := map[NodeStatus]int{
		StatusHealthy:    1,
		StatusSuspect:    0,
		StatusRecovering: 0,
		StatusFailed:     0,
		StatusUnknown:    0,
	}
	for _, peer := range m.peers {
		view := PeerView{
			NodeInfo:          peer.info,
			Status:            peer.status,
			FailureEpoch:      peer.failureEpoch,
			StateSince:        peer.stateSince,
			LastHeartbeat:     peer.lastHeartbeat,
			MissedHeartbeats:  peer.missedHeartbeats,
			HealthyHeartbeats: peer.healthyHeartbeats,
		}
		if !peer.lastHeartbeat.IsZero() {
			view.LastHeartbeatAgeMs = now.Sub(peer.lastHeartbeat).Milliseconds()
		}
		peers = append(peers, view)
		counts[peer.status]++
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	total := 1 + len(m.peers)
	healthy := counts[StatusHealthy]
	quorumAvailable := m.writeQuorum <= 0 || healthy >= m.writeQuorum
	durabilityHealthy := m.replicationFactor <= 0 || healthy >= m.replicationFactor
	degraded := !quorumAvailable || !durabilityHealthy || counts[StatusSuspect] > 0 || counts[StatusRecovering] > 0 || counts[StatusFailed] > 0 || counts[StatusUnknown] > 0
	return Snapshot{
		Self:              self,
		Peers:             peers,
		GeneratedAt:       now,
		TotalNodes:        total,
		HealthyNodes:      healthy,
		SuspectNodes:      counts[StatusSuspect],
		RecoveringNodes:   counts[StatusRecovering],
		FailedNodes:       counts[StatusFailed],
		UnknownNodes:      counts[StatusUnknown],
		ReplicationFactor: m.replicationFactor,
		WriteQuorum:       m.writeQuorum,
		ReadQuorum:        m.readQuorum,
		QuorumAvailable:   quorumAvailable,
		DurabilityHealthy: durabilityHealthy,
		Degraded:          degraded,
	}
}

func (m *Membership) Status(nodeID string) (NodeStatus, bool) {
	if nodeID == m.self.ID {
		return StatusHealthy, true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	peer, ok := m.peers[nodeID]
	if !ok {
		return StatusUnknown, false
	}
	return peer.status, true
}

func (m *Membership) FailureEpoch(nodeID string) (uint64, bool) {
	if nodeID == m.self.ID {
		return 0, true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	peer, ok := m.peers[nodeID]
	if !ok {
		return 0, false
	}
	return peer.failureEpoch, true
}

func (m *Membership) run() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.sendHeartbeats()
	for {
		select {
		case <-ticker.C:
			m.sendHeartbeats()
			m.advanceFailures()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Membership) sendHeartbeats() {
	m.mu.Lock()
	m.sequence++
	seq := m.sequence
	peers := make([]NodeInfo, 0, len(m.peers))
	for _, peer := range m.peers {
		peers = append(peers, peer.info)
	}
	m.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			if m.faults != nil && m.faults.IsBlocked(peer.ID) {
				m.recordMiss(peer.ID)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), m.httpClient.Timeout)
			defer cancel()
			body, err := json.Marshal(HeartbeatRequest{From: m.self, SentAt: time.Now().UTC(), Sequence: seq, Protocol: protocolVersion})
			if err != nil {
				m.recordMiss(peer.ID)
				return
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(peer.Address, "/")+"/internal/v1/heartbeat", bytes.NewReader(body))
			if err != nil {
				m.recordMiss(peer.ID)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := m.httpClient.Do(req)
			if err != nil {
				m.recordMiss(peer.ID)
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				m.recordMiss(peer.ID)
				return
			}
			m.recordSuccess(peer.ID)
		}()
	}
}

func (m *Membership) recordSuccess(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[nodeID]
	if !ok {
		return
	}
	peer.missedHeartbeats = 0
}

func (m *Membership) recordMiss(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	peer, ok := m.peers[nodeID]
	if !ok {
		return
	}
	peer.missedHeartbeats++
}

func (m *Membership) advanceFailures() {
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, peer := range m.peers {
		if peer.lastHeartbeat.IsZero() {
			if peer.missedHeartbeats >= 2 {
				m.beginFailureEpisodeLocked(peer, StatusSuspect, now)
			}
			if peer.missedHeartbeats >= 4 {
				m.transitionLocked(peer, StatusFailed, now)
			}
			continue
		}
		age := now.Sub(peer.lastHeartbeat)
		switch {
		case age >= m.failAfter:
			m.beginFailureEpisodeLocked(peer, StatusFailed, now)
		case age >= m.suspectAfter:
			m.beginFailureEpisodeLocked(peer, StatusSuspect, now)
		case peer.status == StatusSuspect || peer.status == StatusFailed:
			// Do not promote from stale silence to healthy; inbound heartbeat must drive recovery.
		case peer.status == StatusRecovering && age < m.suspectAfter:
		default:
			peer.missedHeartbeats = 0
		}
	}
}

func (m *Membership) beginFailureEpisodeLocked(peer *peerState, status NodeStatus, now time.Time) {
	if !peer.episodeActive {
		peer.failureEpoch++
		peer.episodeActive = true
	}
	m.transitionLocked(peer, status, now)
}

func (m *Membership) transitionLocked(peer *peerState, next NodeStatus, now time.Time) {
	if peer.status == next {
		return
	}
	peer.status = next
	peer.stateSince = now
	if next == StatusRecovering {
		peer.healthyHeartbeats = 1
	}
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func normalizeAddress(address string) string {
	address = strings.TrimSpace(address)
	if address == "" {
		return ""
	}
	if !strings.Contains(address, "://") {
		return "http://" + address
	}
	return strings.TrimRight(address, "/")
}
