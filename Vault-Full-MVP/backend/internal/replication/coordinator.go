package replication

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nitesh/vault/internal/cluster"
	"github.com/nitesh/vault/internal/faults"
	"github.com/nitesh/vault/internal/metadata"
	"github.com/nitesh/vault/internal/storage"
)

var ErrNoReplicas = errors.New("no healthy storage replicas available")
var ErrReplicationFactor = errors.New("replication factor exceeds cluster capacity")
var ErrReplicationUnavailable = errors.New("one or more replicas are unavailable")
var ErrWriteQuorumUnavailable = errors.New("write quorum unavailable")
var ErrReadQuorumUnavailable = errors.New("read quorum unavailable")
var ErrNotLeader = errors.New("not metadata leader")

type NotLeaderError struct {
	LeaderID      string
	LeaderAddress string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "not leader; leader unknown"
	}
	if e.LeaderAddress == "" {
		return "not leader; leader=" + e.LeaderID
	}
	return "not leader; leader=" + e.LeaderID
}

func IsNotLeader(err error) bool {
	var target *NotLeaderError
	return errors.As(err, &target)
}

func LeaderOf(err error) (string, string, bool) {
	var target *NotLeaderError
	if !errors.As(err, &target) {
		return "", "", false
	}
	return target.LeaderID, target.LeaderAddress, true
}

type Config struct {
	Store             *storage.Store
	Membership        *cluster.Membership
	Metadata          *metadata.Service
	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int
	Client            *http.Client
	TempDir           string
	InternalToken     string
	IntegrityDir      string
	Faults            *faults.Injector
}

type Coordinator struct {
	store          *storage.Store
	membership     *cluster.Membership
	metadata       *metadata.Service
	replication    int
	writeQuorum    int
	readQuorum     int
	client         *http.Client
	tempDir        string
	token          string
	integrityDir   string
	faults         *faults.Injector
	quarantineMu   sync.RWMutex
	quarantined    map[string]QuarantineRecord
	repairMu       sync.RWMutex
	repairStats    RepairStats
	rebalanceMu    sync.RWMutex
	rebalanceStats RebalanceStats
	locks          *keyLocker
}

func New(cfg Config) (*Coordinator, error) {
	if cfg.Store == nil || cfg.Membership == nil || cfg.Metadata == nil {
		return nil, errors.New("replication store, membership, and metadata are required")
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.WriteQuorum <= 0 {
		cfg.WriteQuorum = cfg.ReplicationFactor/2 + 1
	}
	if cfg.ReadQuorum <= 0 {
		cfg.ReadQuorum = cfg.ReplicationFactor/2 + 1
	}
	if err := validateQuorumConfig(cfg.ReplicationFactor, cfg.WriteQuorum, cfg.ReadQuorum); err != nil {
		return nil, err
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.TempDir == "" {
		cfg.TempDir = os.TempDir()
	}
	if err := os.MkdirAll(cfg.TempDir, 0o750); err != nil {
		return nil, fmt.Errorf("create replication temp directory: %w", err)
	}
	if cfg.IntegrityDir == "" {
		cfg.IntegrityDir = filepath.Join(filepath.Dir(cfg.TempDir), "integrity")
	}
	if err := os.MkdirAll(cfg.IntegrityDir, 0o750); err != nil {
		return nil, fmt.Errorf("create integrity directory: %w", err)
	}
	quarantined, err := loadQuarantines(cfg.IntegrityDir)
	if err != nil {
		return nil, fmt.Errorf("load quarantine state: %w", err)
	}
	return &Coordinator{
		store:        cfg.Store,
		membership:   cfg.Membership,
		metadata:     cfg.Metadata,
		replication:  cfg.ReplicationFactor,
		writeQuorum:  cfg.WriteQuorum,
		readQuorum:   cfg.ReadQuorum,
		client:       cfg.Client,
		tempDir:      cfg.TempDir,
		token:        cfg.InternalToken,
		integrityDir: cfg.IntegrityDir,
		faults:       cfg.Faults,
		quarantined:  quarantined,
		locks:        newKeyLocker(),
	}, nil
}

func (c *Coordinator) ReplicationFactor() int { return c.replication }
func (c *Coordinator) WriteQuorum() int       { return c.writeQuorum }
func (c *Coordinator) ReadQuorum() int        { return c.readQuorum }
func (c *Coordinator) QuorumStats() QuorumStats {
	return QuorumStats{ReplicationFactor: c.replication, WriteQuorum: c.writeQuorum, ReadQuorum: c.readQuorum}
}
func (c *Coordinator) Store() *storage.Store       { return c.store }
func (c *Coordinator) Metadata() *metadata.Service { return c.metadata }
func (c *Coordinator) AuthorizedInternal(r *http.Request) bool {
	if c.token == "" {
		return true
	}
	got := r.Header.Get("X-Vault-Internal-Token")
	return subtle.ConstantTimeCompare([]byte(got), []byte(c.token)) == 1
}

func (c *Coordinator) Put(ctx context.Context, key, contentType string, src io.Reader) (metadata.Record, error) {
	if err := validateObjectKey(key); err != nil {
		return metadata.Record{}, err
	}
	if err := c.requireLeader(); err != nil {
		return metadata.Record{}, err
	}

	unlock := c.locks.lock(key)
	defer unlock()

	previous, exists := c.metadata.Raw(key)
	version := uint64(1)
	createdAt := time.Now().UTC()
	if exists {
		version = previous.Version + 1
		createdAt = previous.CreatedAt
	}

	tempPath, size, sha256Hex, err := c.stage(src)
	if err != nil {
		return metadata.Record{}, err
	}
	defer os.Remove(tempPath)

	replicas, err := c.chooseReplicas(key)
	if err != nil {
		return metadata.Record{}, err
	}

	type result struct {
		node string
		err  error
	}
	results := make(chan result, len(replicas))
	var wg sync.WaitGroup
	for _, node := range replicas {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- result{node: node.ID, err: c.writeReplica(ctx, node, key, contentType, version, size, sha256Hex, tempPath)}
		}()
	}
	wg.Wait()
	close(results)

	var firstErr error
	written := make([]string, 0, len(replicas))
	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		if result.err == nil {
			written = append(written, result.node)
		}
	}
	if len(written) < c.writeQuorum {
		c.cleanupReplicas(ctx, written, key, version)
		if firstErr != nil {
			return metadata.Record{}, fmt.Errorf("%w: acks=%d required=%d: last error: %v", ErrWriteQuorumUnavailable, len(written), c.writeQuorum, firstErr)
		}
		return metadata.Record{}, fmt.Errorf("%w: acks=%d required=%d", ErrWriteQuorumUnavailable, len(written), c.writeQuorum)
	}

	record := metadata.Record{
		Key:         key,
		Version:     version,
		Size:        size,
		SHA256:      sha256Hex,
		ContentType: contentType,
		Replicas:    replicaIDsByID(written),
		CreatedAt:   createdAt,
		UpdatedAt:   time.Now().UTC(),
	}
	committed, _, err := c.metadata.PutRecord(ctx, record)
	if err != nil {
		c.cleanupReplicas(ctx, written, key, version)
		return metadata.Record{}, err
	}

	// Once the metadata commit is durable, older physical copies are no longer
	// addressable by the object API. Clean them best-effort to bound storage overhead.
	if previous.Version > 0 {
		c.gcOlderVersions(ctx, committed.Replicas, key, committed.Version)
	}
	return committed, nil
}

func (c *Coordinator) Delete(ctx context.Context, key string) error {
	if err := validateObjectKey(key); err != nil {
		return err
	}
	if err := c.requireLeader(); err != nil {
		return err
	}
	unlock := c.locks.lock(key)
	defer unlock()

	record, err := c.metadata.Get(key)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return storage.ErrNotFound
		}
		return err
	}
	for _, nodeID := range record.Replicas {
		node, ok := c.nodeByID(nodeID)
		if !ok {
			return fmt.Errorf("replica node %s is not in cluster membership", nodeID)
		}
		if err := c.deleteReplica(ctx, node, key, record.Version); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("delete replica %s: %w", node.ID, err)
		}
	}
	_, err = c.metadata.Delete(ctx, key)
	return err
}

type ReadSource struct {
	Metadata   metadata.Record
	File       *os.File
	Offset     int64
	Remote     *http.Response
	NodeID     string
	ReadQuorum int
	ReadAcks   int
}

func (s *ReadSource) Close() {
	if s.File != nil {
		_ = s.File.Close()
	}
	if s.Remote != nil && s.Remote.Body != nil {
		_ = s.Remote.Body.Close()
	}
}

// Open returns either a local versioned file or an internal HTTP response from
// another replica. It tries all recorded replicas, preferring the local node.
func (c *Coordinator) Open(ctx context.Context, key, method string, requestHeaders http.Header) (*ReadSource, error) {
	if err := validateObjectKey(key); err != nil {
		return nil, err
	}
	record, err := c.metadataForRead(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(record.Replicas) == 0 {
		return nil, errors.New("metadata has no replica locations")
	}

	ordered := c.orderReplicasForRead(record.Replicas)
	type probeResult struct {
		nodeID string
		match  bool
		err    error
	}
	probes := make(chan probeResult, len(ordered))
	for _, node := range ordered {
		node := node
		go func() {
			ok, err := c.probeReplica(ctx, node, key, record)
			probes <- probeResult{nodeID: node.ID, match: ok, err: err}
		}()
	}
	valid := make(map[string]struct{}, len(ordered))
	var lastErr error
	for range ordered {
		result := <-probes
		if result.match {
			valid[result.nodeID] = struct{}{}
		} else if result.err != nil {
			lastErr = result.err
		}
	}
	if len(valid) < c.readQuorum {
		if lastErr != nil {
			return nil, fmt.Errorf("%w: acks=%d required=%d: last error: %v", ErrReadQuorumUnavailable, len(valid), c.readQuorum, lastErr)
		}
		return nil, fmt.Errorf("%w: acks=%d required=%d", ErrReadQuorumUnavailable, len(valid), c.readQuorum)
	}

	for _, node := range ordered {
		if _, ok := valid[node.ID]; !ok {
			continue
		}
		if node.ID == c.membership.Self().ID {
			file, _, offset, err := c.store.OpenVersion(key, record.Version)
			if err != nil {
				lastErr = err
				continue
			}
			return &ReadSource{Metadata: record, File: file, Offset: offset, NodeID: node.ID, ReadQuorum: c.readQuorum, ReadAcks: len(valid)}, nil
		}
		resp, err := c.fetchRemote(ctx, node, key, record.Version, method, requestHeaders)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent || resp.StatusCode == http.StatusNotModified {
			return &ReadSource{Metadata: record, Remote: resp, NodeID: node.ID, ReadQuorum: c.readQuorum, ReadAcks: len(valid)}, nil
		}
		_ = resp.Body.Close()
		lastErr = fmt.Errorf("replica %s returned HTTP %d", node.ID, resp.StatusCode)
	}
	if lastErr == nil {
		lastErr = ErrNoReplicas
	}
	return nil, fmt.Errorf("read object %q: %w", key, lastErr)
}

func (c *Coordinator) probeReplica(ctx context.Context, node cluster.NodeInfo, key string, record metadata.Record) (bool, error) {
	if q, ok := c.isQuarantined(key, record.Version); ok {
		return false, fmt.Errorf("replica %s quarantined: %s", node.ID, q.Reason)
	}
	if node.ID == c.membership.Self().ID {
		meta, err := c.store.MetadataVersion(key, record.Version)
		if err != nil {
			return false, err
		}
		return meta.Size == record.Size && strings.EqualFold(meta.SHA256, record.SHA256), nil
	}
	resp, err := c.fetchRemote(ctx, node, key, record.Version, http.MethodHead, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("replica %s returned HTTP %d", node.ID, resp.StatusCode)
	}
	gotSize, err := strconv.ParseInt(resp.Header.Get("X-Vault-Object-Size"), 10, 64)
	if err != nil {
		return false, fmt.Errorf("replica %s missing object size", node.ID)
	}
	gotVersion, err := strconv.ParseUint(resp.Header.Get("X-Vault-Object-Version"), 10, 64)
	if err != nil {
		return false, fmt.Errorf("replica %s missing object version", node.ID)
	}
	gotSHA := strings.ToLower(strings.TrimSpace(resp.Header.Get("X-Vault-Object-SHA256")))
	if gotVersion != record.Version || gotSize != record.Size || gotSHA != strings.ToLower(record.SHA256) {
		return false, fmt.Errorf("replica %s metadata mismatch", node.ID)
	}
	return true, nil
}

func (c *Coordinator) metadataForRead(ctx context.Context, key string) (metadata.Record, error) {
	selfID := c.membership.Self().ID
	leaderID, leaderAddress := c.metadata.Raft().Leader()
	if leaderAddress != "" && leaderID != "" && leaderID != selfID {
		endpoint := c.objectURL(leaderAddress, "/v1/metadata/", key)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err == nil {
			resp, err := c.client.Do(req)
			if err == nil {
				defer resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
					var record metadata.Record
					if err := json.NewDecoder(resp.Body).Decode(&record); err == nil {
						return record, nil
					}
				case http.StatusNotFound:
					return metadata.Record{}, metadata.ErrNotFound
				}
			}
		}
	}
	return c.metadata.Get(key)
}

func (c *Coordinator) InternalPut(w http.ResponseWriter, r *http.Request, key string) {
	version, size, sha, contentType, ok := replicaHeaders(r)
	if !ok {
		http.Error(w, "invalid replica headers", http.StatusBadRequest)
		return
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Vault-Repair")), "true") {
		if err := c.store.DeleteVersion(key, version); err != nil && !errors.Is(err, storage.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	meta, created, err := c.store.PutVersion(key, contentType, version, size, sha, r.Body)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, storage.ErrObjectTooLarge):
			status = http.StatusRequestEntityTooLarge
		case errors.Is(err, storage.ErrChecksumMismatch), errors.Is(err, storage.ErrVersionConflict):
			status = http.StatusConflict
		case errors.Is(err, storage.ErrNotFound):
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	_ = c.clearQuarantine(key, version)
	w.Header().Set("ETag", quoteETag(meta.SHA256))
	w.Header().Set("X-Vault-Replica-Version", strconv.FormatUint(meta.Version, 10))
	writeJSON(w, status, meta)
}

func (c *Coordinator) InternalGet(w http.ResponseWriter, r *http.Request, key string) {
	version, err := parseVersion(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, quarantined := c.IsQuarantined(key, version); quarantined {
		http.Error(w, "replica quarantined pending repair", http.StatusLocked)
		return
	}
	file, meta, offset, err := c.store.OpenVersion(key, version)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			http.Error(w, "replica not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()
	section := io.NewSectionReader(file, offset, meta.Size)
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("ETag", quoteETag(meta.SHA256))
	w.Header().Set("X-Vault-Object-Version", strconv.FormatUint(meta.Version, 10))
	w.Header().Set("X-Vault-Object-Size", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("X-Vault-Object-SHA256", meta.SHA256)
	w.Header().Set("X-Vault-Object-Key", meta.Key)
	w.Header().Set("Last-Modified", meta.UpdatedAt.UTC().Format(http.TimeFormat))
	name := meta.Key
	if name == "" {
		name = "object"
	}
	http.ServeContent(w, r, name, meta.UpdatedAt, section)
}

func (c *Coordinator) InternalDelete(w http.ResponseWriter, r *http.Request, key string) {
	version, err := parseVersion(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = c.store.DeleteVersion(key, version)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *Coordinator) InternalGC(w http.ResponseWriter, r *http.Request, key string) {
	keep, err := strconv.ParseUint(r.URL.Query().Get("keep"), 10, 64)
	if err != nil || keep == 0 {
		http.Error(w, "invalid keep version", http.StatusBadRequest)
		return
	}
	if err := c.store.DeleteVersionsBefore(key, keep); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *Coordinator) requireLeader() error {
	status := c.metadata.Raft().Status()
	if status.Role == "leader" && status.ID == c.membership.Self().ID {
		return nil
	}
	leaderID, leaderAddress := c.metadata.Raft().Leader()
	return &NotLeaderError{LeaderID: leaderID, LeaderAddress: leaderAddress}
}

func (c *Coordinator) stage(src io.Reader) (string, int64, string, error) {
	tmp, err := os.CreateTemp(c.tempDir, ".vault-repl-stage-*")
	if err != nil {
		return "", 0, "", fmt.Errorf("create replication staging file: %w", err)
	}
	path := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(path)
	}
	defer func() {
		_ = tmp.Close()
	}()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(src, c.store.MaxObjectSize()+1))
	if err != nil {
		cleanup()
		return "", 0, "", fmt.Errorf("stage object: %w", err)
	}
	if n > c.store.MaxObjectSize() {
		cleanup()
		return "", 0, "", storage.ErrObjectTooLarge
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return "", 0, "", fmt.Errorf("sync replication staging file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", 0, "", fmt.Errorf("close replication staging file: %w", err)
	}
	return path, n, hex.EncodeToString(h.Sum(nil)), nil
}

func (c *Coordinator) chooseReplicas(key string) ([]cluster.NodeInfo, error) {
	snapshot := c.membership.Snapshot()
	nodes := make([]cluster.NodeInfo, 0, len(snapshot.Peers)+1)
	nodes = append(nodes, snapshot.Self.NodeInfo)
	for _, peer := range snapshot.Peers {
		if peer.Status == cluster.StatusHealthy {
			nodes = append(nodes, peer.NodeInfo)
		}
	}
	uniq := make(map[string]cluster.NodeInfo, len(nodes))
	for _, node := range nodes {
		uniq[node.ID] = node
	}
	nodes = nodes[:0]
	for _, node := range uniq {
		nodes = append(nodes, node)
	}
	if len(nodes) < c.writeQuorum {
		return nil, fmt.Errorf("%w: healthy nodes=%d required=%d", ErrWriteQuorumUnavailable, len(nodes), c.writeQuorum)
	}
	type scored struct {
		node  cluster.NodeInfo
		score string
	}
	scoredNodes := make([]scored, 0, len(nodes))
	for _, node := range nodes {
		h := sha256.Sum256([]byte(key + "\x00" + node.ID))
		scoredNodes = append(scoredNodes, scored{node: node, score: hex.EncodeToString(h[:])})
	}
	sort.Slice(scoredNodes, func(i, j int) bool { return scoredNodes[i].score < scoredNodes[j].score })
	limit := c.replication
	if limit > len(scoredNodes) {
		limit = len(scoredNodes)
	}
	out := make([]cluster.NodeInfo, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, scoredNodes[i].node)
	}
	return out, nil
}

func replicaIDsByID(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

func (c *Coordinator) writeReplica(ctx context.Context, node cluster.NodeInfo, key, contentType string, version uint64, size int64, sha, tempPath string) error {
	file, err := os.Open(tempPath)
	if err != nil {
		return err
	}
	defer file.Close()
	if node.ID == c.membership.Self().ID {
		_, _, err := c.store.PutVersion(key, contentType, version, size, sha, file)
		if err == nil {
			_ = c.clearQuarantine(key, version)
		}
		return err
	}
	endpoint := c.objectURL(node.Address, "/internal/v1/replicas/", key)
	query := endpoint.Query()
	query.Set("version", strconv.FormatUint(version, 10))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), file)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Vault-Object-Version", strconv.FormatUint(version, 10))
	req.Header.Set("X-Vault-Object-Size", strconv.FormatInt(size, 10))
	req.Header.Set("X-Vault-Object-SHA256", sha)
	req.Header.Set("X-Vault-Node-ID", c.membership.Self().ID)
	c.addToken(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: node=%s: %v", ErrReplicationUnavailable, node.ID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("replica HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *Coordinator) cleanupReplicas(ctx context.Context, ids []string, key string, version uint64) {
	for _, id := range ids {
		node, ok := c.nodeByID(id)
		if !ok {
			continue
		}
		if id == c.membership.Self().ID {
			_ = c.store.DeleteVersion(key, version)
			continue
		}
		_ = c.deleteReplica(ctx, node, key, version)
	}
}

func (c *Coordinator) deleteReplica(ctx context.Context, node cluster.NodeInfo, key string, version uint64) error {
	endpoint := c.objectURL(node.Address, "/internal/v1/replicas/", key)
	query := endpoint.Query()
	query.Set("version", strconv.FormatUint(version, 10))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Vault-Node-ID", c.membership.Self().ID)
	c.addToken(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return fmt.Errorf("HTTP %d", resp.StatusCode)
}

func (c *Coordinator) gcOlderVersions(ctx context.Context, ids []string, key string, keep uint64) {
	for _, id := range ids {
		node, ok := c.nodeByID(id)
		if !ok {
			continue
		}
		if id == c.membership.Self().ID {
			_ = c.store.DeleteVersionsBefore(key, keep)
			continue
		}
		endpoint := c.objectURL(node.Address, "/internal/v1/replicas/", key)
		query := endpoint.Query()
		query.Set("keep", strconv.FormatUint(keep, 10))
		endpoint.Path = "/internal/v1/replicas-gc/" + strings.TrimPrefix(endpoint.Path, "/internal/v1/replicas/")
		endpoint.RawQuery = query.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), nil)
		if err != nil {
			continue
		}
		c.addToken(req)
		resp, err := c.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}
}

func (c *Coordinator) fetchRemote(ctx context.Context, node cluster.NodeInfo, key string, version uint64, method string, headers http.Header) (*http.Response, error) {
	if c.faults != nil && c.faults.IsBlocked(node.ID) {
		return nil, fmt.Errorf("fault injection: peer %s partitioned", node.ID)
	}
	endpoint := c.objectURL(node.Address, "/internal/v1/replicas/", key)
	query := endpoint.Query()
	query.Set("version", strconv.FormatUint(version, 10))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Node-ID", c.membership.Self().ID)
	for _, name := range []string{"Range", "If-None-Match", "If-Match", "If-Modified-Since", "If-Unmodified-Since", "If-Range"} {
		if value := headers.Get(name); value != "" {
			req.Header.Set(name, value)
		}
	}
	c.addToken(req)
	return c.client.Do(req)
}

func (c *Coordinator) orderReplicasForRead(ids []string) []cluster.NodeInfo {
	selfID := c.membership.Self().ID
	out := make([]cluster.NodeInfo, 0, len(ids))
	if self, ok := c.nodeByID(selfID); ok {
		for _, id := range ids {
			if id == self.ID {
				out = append(out, self)
				break
			}
		}
	}
	for _, id := range ids {
		if id == selfID {
			continue
		}
		if node, ok := c.nodeByID(id); ok {
			out = append(out, node)
		}
	}
	return out
}

func (c *Coordinator) nodeByID(id string) (cluster.NodeInfo, bool) {
	snap := c.membership.Snapshot()
	if snap.Self.ID == id {
		return snap.Self.NodeInfo, true
	}
	for _, peer := range snap.Peers {
		if peer.ID == id {
			return peer.NodeInfo, true
		}
	}
	return cluster.NodeInfo{}, false
}

func (c *Coordinator) objectURL(base, prefix, key string) *url.URL {
	u, _ := url.Parse(strings.TrimRight(base, "/"))
	u.Path = prefix + key
	u.RawQuery = ""
	return u
}

func (c *Coordinator) addToken(req *http.Request) {
	if c.token != "" {
		req.Header.Set("X-Vault-Internal-Token", c.token)
	}
}

func replicaHeaders(r *http.Request) (uint64, int64, string, string, bool) {
	version, err1 := strconv.ParseUint(r.Header.Get("X-Vault-Object-Version"), 10, 64)
	size, err2 := strconv.ParseInt(r.Header.Get("X-Vault-Object-Size"), 10, 64)
	sha := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Vault-Object-SHA256")))
	return version, size, sha, defaultContentType(r.Header.Get("Content-Type")), err1 == nil && err2 == nil && version > 0 && size >= 0 && len(sha) == 64
}

func parseVersion(r *http.Request) (uint64, error) {
	version, err := strconv.ParseUint(r.URL.Query().Get("version"), 10, 64)
	if err != nil || version == 0 {
		return 0, errors.New("invalid object version")
	}
	return version, nil
}

func defaultContentType(value string) string {
	if strings.TrimSpace(value) == "" {
		return "application/octet-stream"
	}
	return value
}

func replicaIDs(nodes []cluster.NodeInfo) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	sort.Strings(ids)
	return ids
}

func validateObjectKey(key string) error {
	if key == "" {
		return errors.New("object key cannot be empty")
	}
	if len(key) > 4096 {
		return errors.New("object key exceeds 4096 bytes")
	}
	if strings.IndexByte(key, 0) >= 0 {
		return errors.New("object key contains NUL byte")
	}
	return nil
}

func quoteETag(value string) string { return `"` + value + `"` }

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type keyLocker struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

func newKeyLocker() *keyLocker { return &keyLocker{locks: make(map[string]*keyLock)} }

func (k *keyLocker) lock(key string) func() {
	k.mu.Lock()
	kl := k.locks[key]
	if kl == nil {
		kl = &keyLock{}
		k.locks[key] = kl
	}
	kl.refs++
	k.mu.Unlock()
	kl.mu.Lock()
	return func() {
		kl.mu.Unlock()
		k.mu.Lock()
		kl.refs--
		if kl.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
