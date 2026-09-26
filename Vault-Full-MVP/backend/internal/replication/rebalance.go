package replication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/nitesh/vault/internal/cluster"
)

type RebalanceStats struct {
	Runs           uint64    `json:"runs"`
	ObjectsScanned uint64    `json:"objects_scanned"`
	ObjectsMoved   uint64    `json:"objects_moved"`
	ReplicasMoved  uint64    `json:"replicas_moved"`
	FailedObjects  uint64    `json:"failed_objects"`
	LastRunAt      time.Time `json:"last_run_at,omitempty"`
	LastSuccessAt  time.Time `json:"last_success_at,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
}

func (c *Coordinator) RebalanceStats() RebalanceStats {
	c.rebalanceMu.RLock()
	defer c.rebalanceMu.RUnlock()
	return c.rebalanceStats
}

func (c *Coordinator) RebalanceAll(ctx context.Context) (RebalanceStats, error) {
	if err := c.requireLeader(); err != nil {
		return RebalanceStats{}, err
	}
	records := c.metadata.ListActive()
	c.recordRebalanceRun()
	local := RebalanceStats{LastRunAt: time.Now().UTC()}
	for _, record := range records {
		if ctx.Err() != nil {
			local.LastError = ctx.Err().Error()
			break
		}
		local.ObjectsScanned++
		moved, err := c.RebalanceObject(ctx, record.Key)
		if err != nil {
			local.FailedObjects++
			local.LastError = err.Error()
			continue
		}
		if moved > 0 {
			local.ObjectsMoved++
			local.ReplicasMoved += uint64(moved)
		}
	}
	c.rebalanceMu.Lock()
	c.rebalanceStats.ObjectsScanned += local.ObjectsScanned
	c.rebalanceStats.ObjectsMoved += local.ObjectsMoved
	c.rebalanceStats.ReplicasMoved += local.ReplicasMoved
	c.rebalanceStats.FailedObjects += local.FailedObjects
	c.rebalanceStats.LastRunAt = local.LastRunAt
	if local.LastError != "" {
		c.rebalanceStats.LastError = local.LastError
	} else {
		c.rebalanceStats.LastSuccessAt = local.LastRunAt
		c.rebalanceStats.LastError = ""
	}
	result := c.rebalanceStats
	c.rebalanceMu.Unlock()
	if local.LastError != "" && local.ObjectsMoved == 0 && local.ObjectsScanned > 0 {
		return result, errors.New(local.LastError)
	}
	return result, nil
}

func (c *Coordinator) RebalanceObject(ctx context.Context, key string) (int, error) {
	if err := validateObjectKey(key); err != nil {
		return 0, err
	}
	if err := c.requireLeader(); err != nil {
		return 0, err
	}
	unlock := c.locks.lock(key)
	defer unlock()

	record, err := c.metadata.Get(key)
	if err != nil {
		return 0, err
	}
	desired, err := c.chooseReplicas(key)
	if err != nil {
		return 0, err
	}
	if len(desired) < c.replication {
		return 0, fmt.Errorf("cannot rebalance %q: healthy placement capacity=%d desired=%d", key, len(desired), c.replication)
	}
	desiredIDs := make([]string, 0, len(desired))
	desiredByID := make(map[string]cluster.NodeInfo, len(desired))
	for _, node := range desired {
		desiredIDs = append(desiredIDs, node.ID)
		desiredByID[node.ID] = node
	}
	desiredIDs = uniqueSorted(desiredIDs)
	if sameStringSet(record.Replicas, desiredIDs) {
		return 0, nil
	}

	report, err := c.Scrub(ctx, key)
	if err != nil {
		return 0, err
	}
	healthy := make(map[string]struct{})
	for _, r := range report.Results {
		if r.Status == IntegrityHealthy {
			healthy[r.NodeID] = struct{}{}
		}
	}
	source, err := c.pickRepairSource(record, healthy)
	if err != nil {
		return 0, err
	}
	moved := 0
	active := make(map[string]struct{})
	for _, id := range record.Replicas {
		if _, ok := healthy[id]; ok {
			active[id] = struct{}{}
		}
	}
	for _, id := range desiredIDs {
		if _, ok := active[id]; ok {
			continue
		}
		target := desiredByID[id]
		if target.ID == source.ID {
			active[target.ID] = struct{}{}
			continue
		}
		if err := c.copyReplicaFromSource(ctx, source, target, record); err != nil {
			return moved, fmt.Errorf("rebalance copy %s -> %s: %w", source.ID, target.ID, err)
		}
		active[target.ID] = struct{}{}
		moved++
		// Prefer a freshly moved healthy target as the next source when useful.
		source = target
	}

	finalIDs := append([]string(nil), desiredIDs...)
	updated, _, err := c.metadata.UpdateReplicas(ctx, record.Key, record.Version, finalIDs)
	if err != nil {
		return moved, err
	}

	keep := make(map[string]struct{}, len(finalIDs))
	for _, id := range finalIDs {
		keep[id] = struct{}{}
	}
	old := append([]string(nil), record.Replicas...)
	sort.Strings(old)
	for _, id := range old {
		if _, ok := keep[id]; ok {
			continue
		}
		if node, ok := c.nodeByID(id); ok {
			status, _ := c.membership.Status(id)
			if status == cluster.StatusHealthy {
				_ = c.deleteReplica(ctx, node, updated.Key, updated.Version)
			}
		}
	}
	return moved, nil
}

func (c *Coordinator) StartRebalancer(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		_, _ = c.RebalanceAll(ctx)
		for {
			select {
			case <-ticker.C:
				_, _ = c.RebalanceAll(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Coordinator) recordRebalanceRun() {
	c.rebalanceMu.Lock()
	c.rebalanceStats.Runs++
	c.rebalanceMu.Unlock()
}
