package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nitesh/vault/internal/faults"
)

const (
	CommandEntry = "command"
)

type Config struct {
	ID          string
	Address     string
	Peers       []Peer
	DataDir     string
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
	RPCTimeout  time.Duration
	RandomSeed  int64
	FSM         FSM
	Faults      *faults.Injector
}

type Node struct {
	mu sync.Mutex

	id      string
	address string
	peers   map[string]Peer
	dataDir string
	fsm     FSM

	currentTerm uint64
	votedFor    string
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64

	role     Role
	leaderID string

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	rpcTimeout  time.Duration
	rnd         *rand.Rand
	faults      *faults.Injector
	electionBy  time.Time

	started  atomic.Bool
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	proposalMu sync.Mutex
}

func New(cfg Config) (*Node, error) {
	if cfg.ID == "" || cfg.Address == "" {
		return nil, errors.New("raft id and address are required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("raft data directory is required")
	}
	if cfg.FSM == nil {
		return nil, errors.New("raft FSM is required")
	}
	if cfg.ElectionMin <= 0 {
		cfg.ElectionMin = 900 * time.Millisecond
	}
	if cfg.ElectionMax <= cfg.ElectionMin {
		cfg.ElectionMax = 1500 * time.Millisecond
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 250 * time.Millisecond
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = 500 * time.Millisecond
	}
	if cfg.RandomSeed == 0 {
		cfg.RandomSeed = time.Now().UnixNano()
	}

	peerMap := make(map[string]Peer)
	for _, p := range cfg.Peers {
		if p.ID == "" || p.Address == "" || p.ID == cfg.ID {
			continue
		}
		if _, ok := peerMap[p.ID]; ok {
			return nil, fmt.Errorf("duplicate peer %q", p.ID)
		}
		peerMap[p.ID] = p
	}

	st, err := loadState(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:          cfg.ID,
		address:     cfg.Address,
		peers:       peerMap,
		dataDir:     cfg.DataDir,
		fsm:         cfg.FSM,
		currentTerm: st.CurrentTerm,
		votedFor:    st.VotedFor,
		log:         append([]LogEntry(nil), st.Log...),
		commitIndex: st.CommitIndex,
		lastApplied: st.CommitIndex,
		role:        RoleFollower,
		leaderID:    "",
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		electionMin: cfg.ElectionMin,
		electionMax: cfg.ElectionMax,
		heartbeat:   cfg.Heartbeat,
		rpcTimeout:  cfg.RPCTimeout,
		rnd:         rand.New(rand.NewSource(cfg.RandomSeed)),
		faults:      cfg.Faults,
		stopCh:      make(chan struct{}),
	}

	if n.commitIndex >= uint64(len(n.log)) {
		return nil, fmt.Errorf("persisted commit index %d exceeds log", n.commitIndex)
	}
	if n.commitIndex > 0 {
		for i := uint64(1); i <= n.commitIndex; i++ {
			if err := n.fsm.Apply(n.log[i]); err != nil {
				return nil, fmt.Errorf("replay committed entry %d: %w", i, err)
			}
			n.lastApplied = i
		}
	}
	n.resetElectionTimerLocked()
	return n, nil
}

func (n *Node) ID() string { return n.id }

func (n *Node) Start() error {
	if n.started.Swap(true) {
		return errors.New("raft already started")
	}
	n.wg.Add(1)
	go n.run()
	return nil
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.wg.Wait()
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	lastIndex, lastTerm := n.lastLogLocked()
	quorum := (len(n.peers)+1)/2 + 1
	return Status{
		ID:            n.id,
		Role:          n.role,
		CurrentTerm:   n.currentTerm,
		LeaderID:      n.leaderID,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LastLogIndex:  lastIndex,
		LastLogTerm:   lastTerm,
		Quorum:        quorum,
		ClusterSize:   len(n.peers) + 1,
		ElectionReset: n.electionBy.UTC().Format(time.RFC3339Nano),
	}
}

func (n *Node) Leader() (string, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == "" {
		return "", ""
	}
	if n.leaderID == n.id {
		return n.id, n.address
	}
	peer, ok := n.peers[n.leaderID]
	if !ok {
		return n.leaderID, ""
	}
	return peer.ID, peer.Address
}

func (n *Node) Peers() []Peer {
	result := make([]Peer, 0, len(n.peers)+1)
	result = append(result, Peer{ID: n.id, Address: n.address})
	for _, p := range n.peers {
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (n *Node) Propose(ctx context.Context, data []byte) (uint64, error) {
	n.proposalMu.Lock()
	defer n.proposalMu.Unlock()

	n.mu.Lock()
	if n.role != RoleLeader {
		leader := n.leaderID
		n.mu.Unlock()
		if leader == "" {
			return 0, errors.New("not leader; leader unknown")
		}
		return 0, fmt.Errorf("not leader; leader=%s", leader)
	}
	idx := uint64(len(n.log))
	entry := LogEntry{Index: idx, Term: n.currentTerm, Type: CommandEntry, Data: append([]byte(nil), data...)}
	n.log = append(n.log, entry)
	if err := n.persistLocked(); err != nil {
		n.log = n.log[:len(n.log)-1]
		n.mu.Unlock()
		return 0, err
	}
	n.mu.Unlock()

	deadline := time.Now().Add(4 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		if time.Now().After(deadline) {
			return 0, errors.New("proposal commit timeout")
		}
		n.replicateAllOnce()
		n.mu.Lock()
		committed := n.commitIndex >= idx && n.role == RoleLeader
		term := n.currentTerm
		leader := n.leaderID
		n.mu.Unlock()
		if committed {
			_ = term
			_ = leader
			// The first replication round can advance commitIndex locally
			// after it has already captured LeaderCommit for its AppendEntries.
			// Broadcast once more so followers learn the committed index before
			// the successful proposal returns to the client.
			n.replicateAllOnce()
			return idx, nil
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func (n *Node) ReceiveRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := RequestVoteResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}
	candidateUpToDate := n.isCandidateLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	if canVote && candidateUpToDate {
		n.votedFor = req.CandidateID
		n.resetElectionTimerLocked()
		if err := n.persistLocked(); err != nil {
			resp.Term = n.currentTerm
			return resp
		}
		resp.VoteGranted = true
	}
	resp.Term = n.currentTerm
	return resp
}

func (n *Node) ReceiveAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := AppendEntriesResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm || n.role != RoleFollower || n.leaderID != req.LeaderID {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	} else {
		n.leaderID = req.LeaderID
		n.resetElectionTimerLocked()
	}
	if req.PrevLogIndex >= uint64(len(n.log)) {
		resp.Term = n.currentTerm
		resp.ConflictAt = uint64(len(n.log))
		return resp
	}
	if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		conflictTerm := n.log[req.PrevLogIndex].Term
		first := req.PrevLogIndex
		for first > 0 && n.log[first-1].Term == conflictTerm {
			first--
		}
		resp.ConflictAt = first
		return resp
	}

	changed := false
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint64(i)
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, entry)
				changed = true
			}
		} else {
			n.log = append(n.log, entry)
			changed = true
		}
	}
	if changed {
		if err := n.persistLocked(); err != nil {
			return resp
		}
	}
	if req.LeaderCommit > n.commitIndex {
		last := uint64(len(n.log) - 1)
		n.commitIndex = min(req.LeaderCommit, last)
		if err := n.applyCommittedLocked(); err != nil {
			return resp
		}
		if err := n.persistLocked(); err != nil {
			return resp
		}
	}
	resp.Success = true
	resp.MatchIndex = req.PrevLogIndex + uint64(len(req.Entries))
	resp.Term = n.currentTerm
	return resp
}

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()
	lastHeartbeat := time.Now()
	for {
		select {
		case <-ticker.C:
			if n.roleNow() == RoleLeader {
				if time.Since(lastHeartbeat) >= n.heartbeat {
					n.replicateAllOnce()
					lastHeartbeat = time.Now()
				}
			} else {
				if time.Now().After(n.electionDeadline()) {
					n.startElection()
				}
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) roleNow() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) electionDeadline() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.electionBy
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	term := n.currentTerm
	n.role = RoleCandidate
	n.leaderID = ""
	n.votedFor = n.id
	lastIndex, lastTerm := n.lastLogLocked()
	n.resetElectionTimerLocked()
	_ = n.persistLocked()
	peers := make([]Peer, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	n.mu.Unlock()

	votes := int32(1)
	quorum := (len(peers)+1)/2 + 1
	if int(votes) >= quorum {
		n.mu.Lock()
		if n.role == RoleCandidate && n.currentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}
	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := n.requestVote(peer, RequestVoteRequest{Term: term, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm})
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				_ = n.persistLocked()
				return
			}
			if n.role != RoleCandidate || n.currentTerm != term || !resp.VoteGranted {
				return
			}
			newVotes := atomic.AddInt32(&votes, 1)
			if int(newVotes) >= quorum {
				n.becomeLeaderLocked()
			}
		}()
	}
	wg.Wait()
}

func (n *Node) becomeLeaderLocked() {
	n.role = RoleLeader
	n.leaderID = n.id
	last := uint64(len(n.log))
	for id := range n.peers {
		n.nextIndex[id] = last
		n.matchIndex[id] = 0
	}
	n.matchIndex[n.id] = last - 1
}

func (n *Node) becomeFollowerLocked(term uint64, leader string) {
	n.currentTerm = term
	n.role = RoleFollower
	n.leaderID = leader
	n.votedFor = ""
	n.resetElectionTimerLocked()
}

func (n *Node) resetElectionTimerLocked() {
	delta := n.electionMin
	if n.electionMax > n.electionMin {
		delta += time.Duration(n.rnd.Int63n(int64(n.electionMax - n.electionMin)))
	}
	n.electionBy = time.Now().Add(delta)
}

func (n *Node) lastLogLocked() (uint64, uint64) {
	idx := uint64(len(n.log) - 1)
	return idx, n.log[idx].Term
}

func (n *Node) isCandidateLogUpToDateLocked(index, term uint64) bool {
	lastIndex, lastTerm := n.lastLogLocked()
	if term != lastTerm {
		return term > lastTerm
	}
	return index >= lastIndex
}

func (n *Node) persistLocked() error {
	return persistState(n.dataDir, persistedState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, Log: append([]LogEntry(nil), n.log...), CommitIndex: n.commitIndex})
}

func (n *Node) applyCommittedLocked() error {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		if err := n.fsm.Apply(entry); err != nil {
			return fmt.Errorf("apply log entry %d: %w", entry.Index, err)
		}
	}
	return nil
}

func (n *Node) replicateAllOnce() {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peers := make([]Peer, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.replicatePeer(peer, term)
		}()
	}
	wg.Wait()
	n.advanceCommit()
}

func (n *Node) replicatePeer(peer Peer, leaderTerm uint64) {
	for attempt := 0; attempt < 3; attempt++ {
		n.mu.Lock()
		if n.role != RoleLeader || n.currentTerm != leaderTerm {
			n.mu.Unlock()
			return
		}
		next := n.nextIndex[peer.ID]
		if next == 0 {
			next = 1
			n.nextIndex[peer.ID] = next
		}
		prev := next - 1
		prevTerm := n.log[prev].Term
		entries := append([]LogEntry(nil), n.log[next:]...)
		commit := n.commitIndex
		n.mu.Unlock()

		resp, err := n.appendEntries(peer, AppendEntriesRequest{Term: leaderTerm, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: commit})
		if err != nil {
			return
		}
		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			_ = n.persistLocked()
			n.mu.Unlock()
			return
		}
		if n.role != RoleLeader || n.currentTerm != leaderTerm {
			n.mu.Unlock()
			return
		}
		if resp.Success {
			match := resp.MatchIndex
			n.matchIndex[peer.ID] = match
			n.nextIndex[peer.ID] = match + 1
			n.mu.Unlock()
			return
		}
		if n.nextIndex[peer.ID] > 1 {
			n.nextIndex[peer.ID]--
		} else {
			n.nextIndex[peer.ID] = 1
		}
		n.mu.Unlock()
	}
}

func (n *Node) advanceCommit() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader {
		return
	}
	last := uint64(len(n.log) - 1)
	for idx := last; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= idx {
				count++
			}
		}
		if count >= (len(n.peers)+1)/2+1 {
			n.commitIndex = idx
			if err := n.applyCommittedLocked(); err == nil {
				_ = n.persistLocked()
			}
			return
		}
	}
}

func (n *Node) requestVote(peer Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
	if n.faults != nil && n.faults.IsBlocked(peer.ID) {
		return RequestVoteResponse{}, errors.New("fault injection: peer partitioned")
	}
	data, _ := json.Marshal(req)
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.Address+"/internal/v1/raft/request-vote", bytes.NewReader(data))
	if err != nil {
		return RequestVoteResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return RequestVoteResponse{}, fmt.Errorf("request vote status %d", resp.StatusCode)
	}
	var out RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return RequestVoteResponse{}, err
	}
	return out, nil
}

func (n *Node) appendEntries(peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if n.faults != nil && n.faults.IsBlocked(peer.ID) {
		return AppendEntriesResponse{}, errors.New("fault injection: peer partitioned")
	}
	data, _ := json.Marshal(req)
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.Address+"/internal/v1/raft/append-entries", bytes.NewReader(data))
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return AppendEntriesResponse{}, fmt.Errorf("append entries status %d", resp.StatusCode)
	}
	var out AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return AppendEntriesResponse{}, err
	}
	return out, nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
