package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nitesh/vault/internal/cluster"
	"github.com/nitesh/vault/internal/metadata"
	"github.com/nitesh/vault/internal/storage"
)

var ErrRepairUnavailable = errors.New("automatic replica repair unavailable")

// RepairStats exposes best-effort repair activity for observability.
type RepairStats struct {
	Attempts          uint64    `json:"attempts"`
	SuccessfulObjects uint64    `json:"successful_objects"`
	FailedObjects     uint64    `json:"failed_objects"`
	RepairedReplicas  uint64    `json:"repaired_replicas"`
	LastError         string    `json:"last_error,omitempty"`
	LastAttemptAt     time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessAt     time.Time `json:"last_success_at,omitempty"`
}

func (c *Coordinator) RepairStats() RepairStats {
	c.repairMu.RLock()
	defer c.repairMu.RUnlock()
	return c.repairStats
}

// RepairObject reconciles one object toward the configured replication factor.
// Healthy replicas are retained, corrupt/missing/failed replicas are replaced,
// and corrupt replicas are safe to overwrite on their original healthy nodes.
func (c *Coordinator) RepairObject(ctx context.Context, key string) (metadata.Record, error) {
	if err := validateObjectKey(key); err != nil {
		return metadata.Record{}, err
	}
	if err := c.requireLeader(); err != nil {
		return metadata.Record{}, err
	}

	unlock := c.locks.lock(key)
	defer unlock()

	c.recordRepairAttempt()
	record, err := c.metadata.Get(key)
	if err != nil {
		c.recordRepairFailure(err)
		return metadata.Record{}, err
	}

	report, err := c.Scrub(ctx, key)
	if err != nil {
		c.recordRepairFailure(err)
		return metadata.Record{}, err
	}

	healthy := make(map[string]struct{}, len(report.Results))
	unhealthy := make(map[string]struct{}, len(record.Replicas))
	for _, result := range report.Results {
		switch result.Status {
		case IntegrityHealthy:
			healthy[result.NodeID] = struct{}{}
		default:
			unhealthy[result.NodeID] = struct{}{}
		}
	}

	// A replica omitted from the report because it has no membership entry is
	// also unhealthy for repair purposes.
	for _, id := range record.Replicas {
		if _, ok := healthy[id]; !ok {
			unhealthy[id] = struct{}{}
		}
	}

	healthyIDs := sortedIDs(healthy)
	if len(healthyIDs) > c.replication {
		healthyIDs = healthyIDs[:c.replication]
	}

	needed := c.replication - len(healthyIDs)
	if needed <= 0 {
		if !sameStringSet(record.Replicas, healthyIDs) {
			updated, _, err := c.metadata.UpdateReplicas(ctx, record.Key, record.Version, healthyIDs)
			if err != nil {
				c.recordRepairFailure(err)
				return metadata.Record{}, err
			}
			c.recordRepairSuccess(0)
			return updated, nil
		}
		c.recordRepairSuccess(0)
		return record, nil
	}

	source, err := c.pickRepairSource(record, healthy)
	if err != nil {
		c.recordRepairFailure(err)
		return metadata.Record{}, err
	}

	targets := c.pickRepairTargets(record, healthy, unhealthy, needed)
	repaired := 0
	finalIDs := append([]string(nil), healthyIDs...)
	for _, target := range targets {
		if ctx.Err() != nil {
			c.recordRepairFailure(ctx.Err())
			return metadata.Record{}, ctx.Err()
		}
		if err := c.copyReplicaFromSource(ctx, source, target, record); err != nil {
			c.recordRepairFailure(fmt.Errorf("repair %s -> %s: %w", source.ID, target.ID, err))
			continue
		}
		finalIDs = append(finalIDs, target.ID)
		repaired++
		_ = c.clearRemoteOrLocalQuarantine(target, record.Key, record.Version)
		if len(finalIDs) >= c.replication {
			break
		}
	}

	finalIDs = uniqueSorted(finalIDs)
	if len(finalIDs) > c.replication {
		finalIDs = finalIDs[:c.replication]
	}
	if !sameStringSet(record.Replicas, finalIDs) {
		updated, _, err := c.metadata.UpdateReplicas(ctx, record.Key, record.Version, finalIDs)
		if err != nil {
			c.recordRepairFailure(err)
			return metadata.Record{}, err
		}
		record = updated
	} else {
		record.Replicas = finalIDs
	}

	if len(finalIDs) < c.replication {
		err := fmt.Errorf("%w: healthy replicas=%d desired=%d repaired=%d", ErrRepairUnavailable, len(finalIDs), c.replication, repaired)
		c.recordRepairFailure(err)
		if repaired > 0 {
			c.recordRepairedReplicas(repaired)
		}
		return record, err
	}

	c.recordRepairSuccess(repaired)
	return record, nil
}

func (c *Coordinator) pickRepairSource(record metadata.Record, healthy map[string]struct{}) (cluster.NodeInfo, error) {
	selfID := c.membership.Self().ID
	if _, ok := healthy[selfID]; ok {
		if node, ok := c.nodeByID(selfID); ok {
			return node, nil
		}
	}
	ids := sortedIDs(healthy)
	for _, id := range ids {
		if node, ok := c.nodeByID(id); ok {
			return node, nil
		}
	}
	return cluster.NodeInfo{}, fmt.Errorf("%w: no verified healthy source replica", ErrRepairUnavailable)
}

func (c *Coordinator) pickRepairTargets(record metadata.Record, healthy, unhealthy map[string]struct{}, needed int) []cluster.NodeInfo {
	existing := make(map[string]struct{}, len(record.Replicas))
	for _, id := range record.Replicas {
		existing[id] = struct{}{}
	}
	snapshot := c.membership.Snapshot()
	candidates := make([]cluster.NodeInfo, 0, len(snapshot.Peers)+1)
	candidates = append(candidates, snapshot.Self.NodeInfo)
	for _, peer := range snapshot.Peers {
		if peer.Status == cluster.StatusHealthy {
			candidates = append(candidates, peer.NodeInfo)
		}
	}
	uniq := make(map[string]cluster.NodeInfo, len(candidates))
	for _, node := range candidates {
		uniq[node.ID] = node
	}
	candidates = candidates[:0]
	for _, node := range uniq {
		candidates = append(candidates, node)
	}
	sort.Slice(candidates, func(i, j int) bool {
		hi := sha256.Sum256([]byte(record.Key + "\x00" + candidates[i].ID))
		hj := sha256.Sum256([]byte(record.Key + "\x00" + candidates[j].ID))
		return hex.EncodeToString(hi[:]) < hex.EncodeToString(hj[:])
	})

	out := make([]cluster.NodeInfo, 0, needed)
	used := make(map[string]struct{})
	// Prefer repairing an online corrupt/quarantined replica in place. This
	// preserves the intended replica topology when the machine is healthy.
	for id := range unhealthy {
		if len(out) >= needed {
			break
		}
		if _, isExisting := existing[id]; !isExisting {
			continue
		}
		if _, alreadyHealthy := healthy[id]; alreadyHealthy {
			continue
		}
		if _, already := used[id]; already {
			continue
		}
		if node, ok := c.nodeByID(id); ok {
			status, _ := c.membership.Status(id)
			if status == cluster.StatusHealthy {
				out = append(out, node)
				used[id] = struct{}{}
			}
		}
	}
	for _, node := range candidates {
		if len(out) >= needed {
			break
		}
		if _, ok := healthy[node.ID]; ok {
			continue
		}
		if _, ok := used[node.ID]; ok {
			continue
		}
		out = append(out, node)
		used[node.ID] = struct{}{}
	}
	return out
}

func (c *Coordinator) copyReplicaFromSource(ctx context.Context, source, target cluster.NodeInfo, record metadata.Record) error {
	if source.ID == target.ID {
		// Source==target can happen only if a caller constructs a bad repair
		// plan. Never overwrite a source with itself.
		return errors.New("repair source and target must differ")
	}

	tempPath, err := c.stageReplicaSource(ctx, source, record)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)
	if err := c.writeRepairReplica(ctx, target, record, tempPath); err != nil {
		return err
	}
	return c.verifyTargetAfterRepair(ctx, target, record)
}

func (c *Coordinator) writeRepairReplica(ctx context.Context, node cluster.NodeInfo, record metadata.Record, tempPath string) error {
	file, err := os.Open(tempPath)
	if err != nil {
		return err
	}
	defer file.Close()
	if node.ID == c.membership.Self().ID {
		if err := c.store.DeleteVersion(record.Key, record.Version); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		_, _, err := c.store.PutVersion(record.Key, record.ContentType, record.Version, record.Size, record.SHA256, file)
		return err
	}
	endpoint := c.objectURL(node.Address, "/internal/v1/replicas/", record.Key)
	query := endpoint.Query()
	query.Set("version", strconv.FormatUint(record.Version, 10))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), file)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", record.ContentType)
	req.Header.Set("X-Vault-Object-Version", strconv.FormatUint(record.Version, 10))
	req.Header.Set("X-Vault-Object-Size", strconv.FormatInt(record.Size, 10))
	req.Header.Set("X-Vault-Object-SHA256", record.SHA256)
	req.Header.Set("X-Vault-Repair", "true")
	c.addToken(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("repair replica HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *Coordinator) stageReplicaSource(ctx context.Context, source cluster.NodeInfo, record metadata.Record) (string, error) {
	temp, err := os.CreateTemp(c.tempDir, ".vault-repair-stage-*")
	if err != nil {
		return "", err
	}
	path := temp.Name()
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(path)
	}

	var src io.Reader
	var closeSource func()
	closeSource = func() {}
	if source.ID == c.membership.Self().ID {
		file, _, offset, err := c.store.OpenVersion(record.Key, record.Version)
		if err != nil {
			cleanup()
			return "", err
		}
		src = io.NewSectionReader(file, offset, record.Size)
		closeSource = func() { _ = file.Close() }
	} else {
		resp, err := c.fetchRemote(ctx, source, record.Key, record.Version, http.MethodGet, nil)
		if err != nil {
			cleanup()
			return "", err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			_ = resp.Body.Close()
			cleanup()
			return "", fmt.Errorf("repair source %s returned HTTP %d", source.ID, resp.StatusCode)
		}
		src = resp.Body
		closeSource = func() { _ = resp.Body.Close() }
	}
	defer closeSource()

	h := sha256.New()
	n, err := io.Copy(temp, io.TeeReader(io.LimitReader(src, record.Size+1), h))
	if err != nil {
		cleanup()
		return "", err
	}
	if n != record.Size {
		cleanup()
		return "", fmt.Errorf("repair source size mismatch: got %d want %d", n, record.Size)
	}
	actual := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(actual, record.SHA256) {
		cleanup()
		return "", fmt.Errorf("repair source checksum mismatch: got %s want %s", actual, record.SHA256)
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := temp.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

func (c *Coordinator) verifyTargetAfterRepair(ctx context.Context, target cluster.NodeInfo, record metadata.Record) error {
	result, err := c.VerifyReplica(ctx, target, record)
	if err != nil {
		return err
	}
	if result.Status != IntegrityHealthy {
		return fmt.Errorf("repaired target %s is not healthy: %s", target.ID, result.Status)
	}
	return nil
}

func (c *Coordinator) clearRemoteOrLocalQuarantine(node cluster.NodeInfo, key string, version uint64) error {
	if node.ID == c.membership.Self().ID {
		return c.clearQuarantine(key, version)
	}
	endpoint := c.objectURL(node.Address, "/internal/v1/integrity/clear/", key)
	query := endpoint.Query()
	query.Set("version", strconv.FormatUint(version, 10))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequest(http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return err
	}
	c.addToken(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("clear quarantine HTTP %d", resp.StatusCode)
	}
	return nil
}

func (c *Coordinator) StartRepairer(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		c.runRepairSweep(ctx)
		for {
			select {
			case <-ticker.C:
				c.runRepairSweep(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Coordinator) runRepairSweep(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if err := c.requireLeader(); err != nil {
		return
	}
	records := c.metadata.ListActive()
	for _, record := range records {
		if ctx.Err() != nil {
			return
		}
		_, _ = c.RepairObject(ctx, record.Key)
	}
}

func (c *Coordinator) recordRepairAttempt() {
	c.repairMu.Lock()
	defer c.repairMu.Unlock()
	c.repairStats.Attempts++
	c.repairStats.LastAttemptAt = time.Now().UTC()
}

func (c *Coordinator) recordRepairSuccess(repaired int) {
	c.repairMu.Lock()
	defer c.repairMu.Unlock()
	c.repairStats.SuccessfulObjects++
	c.repairStats.RepairedReplicas += uint64(repaired)
	c.repairStats.LastSuccessAt = time.Now().UTC()
	c.repairStats.LastError = ""
}

func (c *Coordinator) recordRepairedReplicas(repaired int) {
	c.repairMu.Lock()
	defer c.repairMu.Unlock()
	c.repairStats.RepairedReplicas += uint64(repaired)
}

func (c *Coordinator) recordRepairFailure(err error) {
	c.repairMu.Lock()
	defer c.repairMu.Unlock()
	c.repairStats.FailedObjects++
	c.repairStats.LastError = err.Error()
}

func sortedIDs(ids map[string]struct{}) []string {
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func uniqueSorted(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func sameStringSet(a, b []string) bool {
	aa := uniqueSorted(a)
	bb := uniqueSorted(b)
	if len(aa) != len(bb) {
		return false
	}
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
