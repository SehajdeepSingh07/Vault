package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nitesh/vault/internal/cluster"
	"github.com/nitesh/vault/internal/metadata"
	"github.com/nitesh/vault/internal/storage"
)

type IntegrityStatus string

const (
	IntegrityHealthy     IntegrityStatus = "healthy"
	IntegrityCorrupt     IntegrityStatus = "corrupt"
	IntegrityMissing     IntegrityStatus = "missing"
	IntegrityError       IntegrityStatus = "error"
	IntegrityQuarantined IntegrityStatus = "quarantined"
)

type QuarantineRecord struct {
	Key            string    `json:"key"`
	Version        uint64    `json:"version"`
	ExpectedSize   int64     `json:"expected_size"`
	ExpectedSHA256 string    `json:"expected_sha256"`
	ActualSize     int64     `json:"actual_size,omitempty"`
	ActualSHA256   string    `json:"actual_sha256,omitempty"`
	Reason         string    `json:"reason"`
	DetectedAt     time.Time `json:"detected_at"`
}

type ReplicaIntegrityResult struct {
	NodeID         string          `json:"node_id"`
	Status         IntegrityStatus `json:"status"`
	Key            string          `json:"key"`
	Version        uint64          `json:"version"`
	ExpectedSize   int64           `json:"expected_size"`
	ActualSize     int64           `json:"actual_size"`
	ExpectedSHA256 string          `json:"expected_sha256"`
	ActualSHA256   string          `json:"actual_sha256"`
	BytesVerified  int64           `json:"bytes_verified"`
	Quarantined    bool            `json:"quarantined"`
	Reason         string          `json:"reason,omitempty"`
	CheckedAt      time.Time       `json:"checked_at"`
}

type IntegrityReport struct {
	Key            string                   `json:"key"`
	Version        uint64                   `json:"version"`
	ExpectedSize   int64                    `json:"expected_size"`
	ExpectedSHA256 string                   `json:"expected_sha256"`
	CheckedAt      time.Time                `json:"checked_at"`
	HealthyAcks    int                      `json:"healthy_acks"`
	CorruptCount   int                      `json:"corrupt_count"`
	MissingCount   int                      `json:"missing_count"`
	ErrorCount     int                      `json:"error_count"`
	Results        []ReplicaIntegrityResult `json:"results"`
}

type IntegritySnapshot struct {
	NodeID      string             `json:"node_id"`
	Quarantines []QuarantineRecord `json:"quarantines"`
	GeneratedAt time.Time          `json:"generated_at"`
}

func quarantineKey(key string, version uint64) string {
	return key + "\x00" + strconv.FormatUint(version, 10)
}

// ClearQuarantine removes local quarantine state after a successful verified repair.
func (c *Coordinator) ClearQuarantine(key string, version uint64) error {
	return c.clearQuarantine(key, version)
}

func (c *Coordinator) IsQuarantined(key string, version uint64) (QuarantineRecord, bool) {
	return c.isQuarantined(key, version)
}

func (c *Coordinator) isQuarantined(key string, version uint64) (QuarantineRecord, bool) {
	c.quarantineMu.RLock()
	defer c.quarantineMu.RUnlock()
	q, ok := c.quarantined[quarantineKey(key, version)]
	return q, ok
}

func (c *Coordinator) markQuarantined(q QuarantineRecord) error {
	c.quarantineMu.Lock()
	defer c.quarantineMu.Unlock()
	c.quarantined[quarantineKey(q.Key, q.Version)] = q
	return c.persistQuarantineLocked()
}

func (c *Coordinator) clearQuarantine(key string, version uint64) error {
	c.quarantineMu.Lock()
	defer c.quarantineMu.Unlock()
	delete(c.quarantined, quarantineKey(key, version))
	return c.persistQuarantineLocked()
}

func (c *Coordinator) persistQuarantineLocked() error {
	if c.integrityDir == "" {
		return nil
	}
	records := make([]QuarantineRecord, 0, len(c.quarantined))
	for _, q := range c.quarantined {
		records = append(records, q)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Key != records[j].Key {
			return records[i].Key < records[j].Key
		}
		return records[i].Version < records[j].Version
	})
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.integrityDir, ".quarantine-*")
	if err != nil {
		return err
	}
	path := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(path)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(c.integrityDir, "quarantine.json")); err != nil {
		return err
	}
	ok = true
	return nil
}

func loadQuarantines(dir string) (map[string]QuarantineRecord, error) {
	out := make(map[string]QuarantineRecord)
	if dir == "" {
		return out, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, "quarantine.json"))
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var records []QuarantineRecord
	if len(data) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	for _, q := range records {
		out[quarantineKey(q.Key, q.Version)] = q
	}
	return out, nil
}

func (c *Coordinator) VerifyLocal(ctx context.Context, record metadata.Record) ReplicaIntegrityResult {
	_ = ctx
	checked := time.Now().UTC()
	result, err := c.store.VerifyVersion(record.Key, record.Version)
	if err != nil {
		status := IntegrityCorrupt
		if errors.Is(err, storage.ErrNotFound) {
			status = IntegrityMissing
		}
		out := ReplicaIntegrityResult{NodeID: c.membership.Self().ID, Status: status, Key: record.Key, Version: record.Version, ExpectedSize: record.Size, ExpectedSHA256: record.SHA256, CheckedAt: checked, Reason: err.Error()}
		if status == IntegrityCorrupt {
			q := QuarantineRecord{Key: record.Key, Version: record.Version, ExpectedSize: record.Size, ExpectedSHA256: record.SHA256, Reason: err.Error(), DetectedAt: checked}
			if qerr := c.markQuarantined(q); qerr == nil {
				out.Quarantined = true
				out.Status = IntegrityQuarantined
			}
		}
		return out
	}
	out := ReplicaIntegrityResult{
		NodeID: c.membership.Self().ID, Status: IntegrityHealthy, Key: record.Key, Version: record.Version,
		ExpectedSize: result.ExpectedSize, ActualSize: result.ActualSize, ExpectedSHA256: result.ExpectedSHA256,
		ActualSHA256: result.ActualSHA256, BytesVerified: result.BytesVerified, CheckedAt: checked,
	}
	if !result.Valid {
		out.Status = IntegrityCorrupt
		out.Reason = result.Reason
		q := QuarantineRecord{Key: record.Key, Version: record.Version, ExpectedSize: result.ExpectedSize, ExpectedSHA256: result.ExpectedSHA256, ActualSize: result.ActualSize, ActualSHA256: result.ActualSHA256, Reason: result.Reason, DetectedAt: checked}
		if qerr := c.markQuarantined(q); qerr == nil {
			out.Quarantined = true
			out.Status = IntegrityQuarantined
		}
	} else {
		_ = c.clearQuarantine(record.Key, record.Version)
	}
	return out
}

func (c *Coordinator) VerifyReplica(ctx context.Context, node cluster.NodeInfo, record metadata.Record) (ReplicaIntegrityResult, error) {
	if node.ID == c.membership.Self().ID {
		return c.VerifyLocal(ctx, record), nil
	}
	endpoint := c.objectURL(node.Address, "/internal/v1/integrity/verify/", record.Key)
	query := endpoint.Query()
	query.Set("version", strconv.FormatUint(record.Version, 10))
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return ReplicaIntegrityResult{}, err
	}
	req.Header.Set("X-Vault-Expected-Size", strconv.FormatInt(record.Size, 10))
	req.Header.Set("X-Vault-Expected-SHA256", record.SHA256)
	req.Header.Set("X-Vault-Expected-Key", record.Key)
	c.addToken(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return ReplicaIntegrityResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ReplicaIntegrityResult{}, fmt.Errorf("integrity verify HTTP %d", resp.StatusCode)
	}
	var result ReplicaIntegrityResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ReplicaIntegrityResult{}, err
	}
	return result, nil
}

func (c *Coordinator) InternalIntegrityVerify(w http.ResponseWriter, r *http.Request, key string) {
	version, err := parseVersion(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	expectedSize, err := strconv.ParseInt(r.Header.Get("X-Vault-Expected-Size"), 10, 64)
	if err != nil || expectedSize < 0 {
		http.Error(w, "invalid expected size", http.StatusBadRequest)
		return
	}
	expectedSHA := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Vault-Expected-SHA256")))
	if len(expectedSHA) != 64 {
		http.Error(w, "invalid expected sha256", http.StatusBadRequest)
		return
	}
	record := metadata.Record{Key: key, Version: version, Size: expectedSize, SHA256: expectedSHA}
	result := c.VerifyLocal(r.Context(), record)
	writeJSON(w, http.StatusOK, result)
}

func (c *Coordinator) Scrub(ctx context.Context, key string) (IntegrityReport, error) {
	if err := validateObjectKey(key); err != nil {
		return IntegrityReport{}, err
	}
	record, err := c.metadataForRead(ctx, key)
	if err != nil {
		return IntegrityReport{}, err
	}
	report := IntegrityReport{Key: key, Version: record.Version, ExpectedSize: record.Size, ExpectedSHA256: record.SHA256, CheckedAt: time.Now().UTC(), Results: make([]ReplicaIntegrityResult, 0, len(record.Replicas))}
	type response struct {
		result ReplicaIntegrityResult
		err    error
	}
	ch := make(chan response, len(record.Replicas))
	var wg sync.WaitGroup
	for _, id := range record.Replicas {
		node, ok := c.nodeByID(id)
		if !ok {
			ch <- response{result: ReplicaIntegrityResult{NodeID: id, Status: IntegrityError, Key: key, Version: record.Version, ExpectedSize: record.Size, ExpectedSHA256: record.SHA256, CheckedAt: time.Now().UTC(), Reason: "replica node is not in membership"}}
			continue
		}
		wg.Add(1)
		go func(node cluster.NodeInfo) {
			defer wg.Done()
			result, err := c.VerifyReplica(ctx, node, record)
			ch <- response{result: result, err: err}
		}(node)
	}
	wg.Wait()
	close(ch)
	for item := range ch {
		if item.err != nil {
			item.result = ReplicaIntegrityResult{NodeID: "unknown", Status: IntegrityError, Key: key, Version: record.Version, ExpectedSize: record.Size, ExpectedSHA256: record.SHA256, CheckedAt: time.Now().UTC(), Reason: item.err.Error()}
		}
		report.Results = append(report.Results, item.result)
	}
	sort.Slice(report.Results, func(i, j int) bool { return report.Results[i].NodeID < report.Results[j].NodeID })
	for _, result := range report.Results {
		switch result.Status {
		case IntegrityHealthy:
			report.HealthyAcks++
		case IntegrityCorrupt, IntegrityQuarantined:
			report.CorruptCount++
		case IntegrityMissing:
			report.MissingCount++
		default:
			report.ErrorCount++
		}
	}
	return report, nil
}

func (c *Coordinator) IntegritySnapshot() IntegritySnapshot {
	c.quarantineMu.RLock()
	defer c.quarantineMu.RUnlock()
	list := make([]QuarantineRecord, 0, len(c.quarantined))
	for _, q := range c.quarantined {
		list = append(list, q)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Key != list[j].Key {
			return list[i].Key < list[j].Key
		}
		return list[i].Version < list[j].Version
	})
	return IntegritySnapshot{NodeID: c.membership.Self().ID, Quarantines: list, GeneratedAt: time.Now().UTC()}
}

func (c *Coordinator) StartScrubber(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		c.runLocalScrub(ctx)
		for {
			select {
			case <-ticker.C:
				c.runLocalScrub(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Coordinator) runLocalScrub(ctx context.Context) {
	records := c.metadata.ListActive()
	self := c.membership.Self().ID
	for _, record := range records {
		if ctx.Err() != nil {
			return
		}
		found := false
		for _, id := range record.Replicas {
			if id == self {
				found = true
				break
			}
		}
		if found {
			_ = c.VerifyLocal(ctx, record)
		}
	}
}
