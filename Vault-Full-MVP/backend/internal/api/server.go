package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nitesh/vault/internal/cluster"
	"github.com/nitesh/vault/internal/faults"
	"github.com/nitesh/vault/internal/metadata"
	"github.com/nitesh/vault/internal/raft"
	"github.com/nitesh/vault/internal/replication"
	"github.com/nitesh/vault/internal/storage"
)

type Server struct {
	store   *storage.Store
	cluster *cluster.Membership
	meta    *metadata.Service
	repl    *replication.Coordinator
	faults  *faults.Injector
}

func New(store *storage.Store, memberships ...*cluster.Membership) *Server {
	var membership *cluster.Membership
	if len(memberships) > 0 {
		membership = memberships[0]
	}
	return &Server{store: store, cluster: membership}
}

func NewWithMetadata(store *storage.Store, membership *cluster.Membership, meta *metadata.Service) *Server {
	return &Server{store: store, cluster: membership, meta: meta}
}

func NewWithMetadataAndReplication(store *storage.Store, membership *cluster.Membership, meta *metadata.Service, repl *replication.Coordinator, injectors ...*faults.Injector) *Server {
	var injector *faults.Injector
	if len(injectors) > 0 {
		injector = injectors[0]
	}
	return &Server{store: store, cluster: membership, meta: meta, repl: repl, faults: injector}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.root)
	mux.HandleFunc("/v1/health", s.health)
	mux.HandleFunc("/v1/version", s.version)
	mux.HandleFunc("/v1/node", s.node)
	mux.HandleFunc("/v1/cluster", s.clusterStatus)
	mux.HandleFunc("/v1/faults", s.faultsRoot)
	mux.HandleFunc("/v1/rebalance", s.rebalance)
	mux.HandleFunc("/v1/rebalance/", s.rebalanceObject)
	mux.HandleFunc("/v1/raft", s.raftStatus)
	mux.HandleFunc("/v1/integrity", s.integrityRoot)
	mux.HandleFunc("/v1/integrity/", s.integrity)
	if s.repl != nil {
		mux.HandleFunc("/v1/repair", s.repairRoot)
		mux.HandleFunc("/v1/repair/", s.repair)
	}
	mux.HandleFunc("/v1/metadata", s.metadataRoot)
	mux.HandleFunc("/v1/metadata/", s.metadata)
	mux.HandleFunc("/internal/v1/heartbeat", s.heartbeat)
	mux.HandleFunc("/internal/v1/raft/request-vote", s.requestVote)
	mux.HandleFunc("/internal/v1/raft/append-entries", s.appendEntries)
	if s.repl != nil {
		mux.HandleFunc("/internal/v1/replicas/", s.replica)
		mux.HandleFunc("/internal/v1/replicas-gc/", s.replicaGC)
		mux.HandleFunc("/internal/v1/integrity/verify/", s.integrityVerify)
		mux.HandleFunc("/internal/v1/integrity/clear/", s.integrityClear)
	}
	mux.HandleFunc("/v1/objects/", s.object)
	mux.HandleFunc("/v1/objects", s.objectsRoot)
	return requestID(requestLogger(mux))
}

func (s *Server) root(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service":       "vault",
		"api":           "/v1",
		"version":       "1.0.0-mvp",
		"documentation": "docs/openapi.yaml",
	})
}

func (s *Server) version(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service":     "vault",
		"version":     "1.0.0-mvp",
		"api_version": "v1",
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	payload := map[string]any{
		"status":  "ok",
		"service": "vault",
		"time":    time.Now().UTC().Format(time.RFC3339Nano),
	}
	if s.cluster != nil {
		snap := s.cluster.Snapshot()
		payload["node_id"] = snap.Self.ID
		payload["node_status"] = snap.Self.Status
		payload["cluster_degraded"] = snap.Degraded
		payload["healthy_nodes"] = snap.HealthyNodes
		payload["failed_nodes"] = snap.FailedNodes
		payload["suspect_nodes"] = snap.SuspectNodes
		payload["recovering_nodes"] = snap.RecoveringNodes
		payload["quorum_available"] = snap.QuorumAvailable
		payload["durability_healthy"] = snap.DurabilityHealthy
	}
	if s.meta != nil {
		payload["raft_role"] = s.meta.Raft().Status().Role
	}
	if s.repl != nil {
		stats := s.repl.QuorumStats()
		payload["replication_factor"] = stats.ReplicationFactor
		payload["write_quorum"] = stats.WriteQuorum
		payload["read_quorum"] = stats.ReadQuorum
		payload["integrity_quarantines"] = len(s.repl.IntegritySnapshot().Quarantines)
	}
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) node(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if s.cluster == nil {
		http.Error(w, "cluster membership is disabled", http.StatusNotImplemented)
		return
	}
	writeJSON(w, http.StatusOK, s.cluster.Snapshot().Self)
}

func (s *Server) clusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if s.cluster == nil {
		http.Error(w, "cluster membership is disabled", http.StatusNotImplemented)
		return
	}
	writeJSON(w, http.StatusOK, s.cluster.Snapshot())
}

func (s *Server) raftStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if s.meta == nil {
		http.Error(w, "metadata raft is disabled", http.StatusNotImplemented)
		return
	}
	status := s.meta.Raft().Status()
	leaderID, leaderAddress := s.meta.Raft().Leader()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         status,
		"leader_id":      leaderID,
		"leader_address": leaderAddress,
		"peers":          s.meta.Raft().Peers(),
	})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if s.cluster == nil {
		http.Error(w, "cluster membership is disabled", http.StatusNotImplemented)
		return
	}
	var req cluster.HeartbeatRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid heartbeat", http.StatusBadRequest)
		return
	}
	if s.faults != nil && s.faults.IsBlocked(req.From.ID) {
		http.Error(w, "fault injection: heartbeat partitioned", http.StatusServiceUnavailable)
		return
	}
	if err := s.cluster.ReceiveHeartbeat(req); err != nil {
		status := http.StatusForbidden
		if strings.Contains(err.Error(), "protocol") {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ack", "node": s.cluster.Self().ID})
}

func (s *Server) requestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if s.meta == nil {
		http.Error(w, "metadata raft is disabled", http.StatusNotImplemented)
		return
	}
	var req raft.RequestVoteRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 256<<10)).Decode(&req); err != nil {
		http.Error(w, "invalid request vote", http.StatusBadRequest)
		return
	}
	if s.faults != nil && s.faults.IsBlocked(req.CandidateID) {
		http.Error(w, "fault injection: vote partitioned", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, s.meta.Raft().ReceiveRequestVote(req))
}

func (s *Server) appendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if s.meta == nil {
		http.Error(w, "metadata raft is disabled", http.StatusNotImplemented)
		return
	}
	var req raft.AppendEntriesRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid append entries", http.StatusBadRequest)
		return
	}
	if s.faults != nil && s.faults.IsBlocked(req.LeaderID) {
		http.Error(w, "fault injection: append partitioned", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, s.meta.Raft().ReceiveAppendEntries(req))
}

func (s *Server) metadataRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/metadata" || r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if s.meta == nil {
		http.Error(w, "metadata raft is disabled", http.StatusNotImplemented)
		return
	}
	writeJSON(w, http.StatusOK, s.meta.ListActive())
}

func (s *Server) metadata(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil {
		http.Error(w, "metadata raft is disabled", http.StatusNotImplemented)
		return
	}
	const prefix = "/v1/metadata/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if key == "" {
		http.Error(w, "metadata key is required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		record, err := s.meta.Get(key)
		if err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				http.Error(w, "metadata not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, record)
	case http.MethodPut:
		var req struct {
			Size        int64  `json:"size"`
			SHA256      string `json:"sha256"`
			ContentType string `json:"content_type"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
			http.Error(w, "invalid metadata request", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		record, index, err := s.meta.Put(ctx, key, req.Size, req.SHA256, req.ContentType)
		if err != nil {
			s.writeRaftError(w, err)
			return
		}
		w.Header().Set("X-Vault-Raft-Index", fmt.Sprint(index))
		writeJSON(w, http.StatusOK, record)
	case http.MethodDelete:
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		index, err := s.meta.Delete(ctx, key)
		if err != nil {
			s.writeRaftError(w, err)
			return
		}
		w.Header().Set("X-Vault-Raft-Index", fmt.Sprint(index))
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (s *Server) writeRaftError(w http.ResponseWriter, err error) {
	if strings.HasPrefix(err.Error(), "not leader") {
		leaderID, leaderAddress := s.meta.Raft().Leader()
		if leaderID != "" {
			w.Header().Set("X-Vault-Leader-ID", leaderID)
		}
		if leaderAddress != "" {
			w.Header().Set("X-Vault-Leader-Address", leaderAddress)
		}
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (s *Server) objectsRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/objects" {
		http.NotFound(w, r)
		return
	}
	http.Error(w, "object key is required", http.StatusBadRequest)
}

func (s *Server) object(w http.ResponseWriter, r *http.Request) {
	const prefix = "/v1/objects/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if key == "" {
		http.Error(w, "object key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.put(w, r, key)
	case http.MethodGet:
		s.get(w, r, key)
	case http.MethodHead:
		s.head(w, r, key)
	case http.MethodDelete:
		s.delete(w, r, key)
	default:
		methodNotAllowed(w, http.MethodPut, http.MethodGet, http.MethodHead, http.MethodDelete)
	}
}

func (s *Server) put(w http.ResponseWriter, r *http.Request, key string) {
	if s.repl != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		record, err := s.repl.Put(ctx, key, r.Header.Get("Content-Type"), r.Body)
		if err != nil {
			s.writeReplicationError(w, r, key, err)
			return
		}
		writeObjectHeaders(w, record)
		w.Header().Set("X-Vault-Replica-Count", fmt.Sprint(len(record.Replicas)))
		w.Header().Set("X-Vault-Write-Quorum", fmt.Sprint(s.repl.WriteQuorum()))
		w.Header().Set("X-Vault-Write-Acks", fmt.Sprint(len(record.Replicas)))
		w.WriteHeader(http.StatusCreated)
		return
	}

	meta, err := s.store.Put(key, r.Header.Get("Content-Type"), r.Body)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, storage.ErrObjectTooLarge):
			status = http.StatusRequestEntityTooLarge
		case strings.Contains(err.Error(), "key"):
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.Header().Set("ETag", quoteETag(meta.SHA256))
	w.Header().Set("X-Vault-Object-Version", fmt.Sprint(meta.Version))
	w.Header().Set("X-Vault-Object-Size", fmt.Sprint(meta.Size))
	w.Header().Set("X-Vault-Object-SHA256", meta.SHA256)
	w.Header().Set("X-Vault-Object-Key", meta.Key)
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, key string) {
	if s.repl != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		source, err := s.repl.Open(ctx, key, http.MethodGet, r.Header)
		if err != nil {
			s.writeStorageError(w, err)
			return
		}
		defer source.Close()
		if source.Remote != nil {
			w.Header().Set("X-Vault-Read-Quorum", fmt.Sprint(source.ReadQuorum))
			w.Header().Set("X-Vault-Read-Acks", fmt.Sprint(source.ReadAcks))
			proxyResponse(w, source.Remote, true)
			return
		}
		setObjectHeaders(w, source.Metadata)
		w.Header().Set("X-Vault-Read-Quorum", fmt.Sprint(source.ReadQuorum))
		w.Header().Set("X-Vault-Read-Acks", fmt.Sprint(source.ReadAcks))
		section := io.NewSectionReader(source.File, source.Offset, source.Metadata.Size)
		http.ServeContent(w, r, source.Metadata.Key, source.Metadata.UpdatedAt, section)
		return
	}

	file, meta, offset, err := s.store.Open(key)
	if err != nil {
		s.writeStorageError(w, err)
		return
	}
	defer file.Close()

	section := io.NewSectionReader(file, offset, meta.Size)
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("ETag", quoteETag(meta.SHA256))
	w.Header().Set("X-Vault-Object-Version", fmt.Sprint(meta.Version))
	w.Header().Set("X-Vault-Object-Size", fmt.Sprint(meta.Size))
	w.Header().Set("X-Vault-Object-SHA256", meta.SHA256)
	w.Header().Set("X-Vault-Object-Key", meta.Key)
	w.Header().Set("Last-Modified", meta.UpdatedAt.UTC().Format(http.TimeFormat))

	name := meta.Key
	if name == "" {
		name = "object"
	}
	http.ServeContent(w, r, name, meta.UpdatedAt, section)
}

func (s *Server) head(w http.ResponseWriter, r *http.Request, key string) {
	if s.repl != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		source, err := s.repl.Open(ctx, key, http.MethodHead, r.Header)
		if err != nil {
			s.writeStorageError(w, err)
			return
		}
		defer source.Close()
		if source.Remote != nil {
			w.Header().Set("X-Vault-Read-Quorum", fmt.Sprint(source.ReadQuorum))
			w.Header().Set("X-Vault-Read-Acks", fmt.Sprint(source.ReadAcks))
			proxyResponse(w, source.Remote, false)
			return
		}
		setObjectHeaders(w, source.Metadata)
		w.Header().Set("X-Vault-Read-Quorum", fmt.Sprint(source.ReadQuorum))
		w.Header().Set("X-Vault-Read-Acks", fmt.Sprint(source.ReadAcks))
		section := io.NewSectionReader(source.File, source.Offset, source.Metadata.Size)
		http.ServeContent(w, r, source.Metadata.Key, source.Metadata.UpdatedAt, section)
		return
	}

	meta, err := s.store.Metadata(key)
	if err != nil {
		s.writeStorageError(w, err)
		return
	}
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", fmt.Sprint(meta.Size))
	w.Header().Set("ETag", quoteETag(meta.SHA256))
	w.Header().Set("X-Vault-Object-Version", fmt.Sprint(meta.Version))
	w.Header().Set("X-Vault-Object-Size", fmt.Sprint(meta.Size))
	w.Header().Set("X-Vault-Object-SHA256", meta.SHA256)
	w.Header().Set("X-Vault-Object-Key", meta.Key)
	w.Header().Set("Last-Modified", meta.UpdatedAt.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request, key string) {
	if s.repl != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		if err := s.repl.Delete(ctx, key); err != nil {
			s.writeReplicationError(w, r, key, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := s.store.Delete(key); err != nil {
		s.writeStorageError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) repairRoot(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil {
		http.Error(w, "repair service is disabled", http.StatusNotImplemented)
		return
	}
	if r.URL.Path != "/v1/repair" || r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, s.repl.RepairStats())
}

func (s *Server) repair(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil {
		http.Error(w, "repair service is disabled", http.StatusNotImplemented)
		return
	}
	const prefix = "/v1/repair/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if key == "" {
		http.Error(w, "object key is required", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
		return
	}
	if r.Method == http.MethodGet {
		record, err := s.repl.Metadata().Get(key)
		if err != nil {
			s.writeReplicationError(w, r, key, err)
			return
		}
		writeJSON(w, http.StatusOK, record)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	record, err := s.repl.RepairObject(ctx, key)
	if err != nil {
		s.writeReplicationError(w, r, key, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) integrityRoot(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil {
		http.Error(w, "integrity service is disabled", http.StatusNotImplemented)
		return
	}
	if r.URL.Path != "/v1/integrity" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, s.repl.IntegritySnapshot())
}

func (s *Server) integrity(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil {
		http.Error(w, "integrity service is disabled", http.StatusNotImplemented)
		return
	}
	const prefix = "/v1/integrity/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if key == "" {
		http.Error(w, "object key is required", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	report, err := s.repl.Scrub(ctx, key)
	if err != nil {
		s.writeReplicationError(w, r, key, err)
		return
	}
	status := http.StatusOK
	if report.CorruptCount > 0 || report.MissingCount > 0 || report.ErrorCount > 0 {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, report)
}

func (s *Server) integrityVerify(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil || !s.repl.AuthorizedInternal(r) {
		http.Error(w, "unauthorized internal request", http.StatusUnauthorized)
		return
	}
	const prefix = "/internal/v1/integrity/verify/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if r.Method != http.MethodPost || key == "" {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	s.repl.InternalIntegrityVerify(w, r, key)
}

func (s *Server) integrityClear(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil || !s.repl.AuthorizedInternal(r) {
		http.Error(w, "unauthorized internal request", http.StatusUnauthorized)
		return
	}
	const prefix = "/internal/v1/integrity/clear/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if r.Method != http.MethodPost || key == "" {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	version, err := strconv.ParseUint(r.URL.Query().Get("version"), 10, 64)
	if err != nil || version == 0 {
		http.Error(w, "invalid version", http.StatusBadRequest)
		return
	}
	if err := s.repl.ClearQuarantine(key, version); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) replica(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil || !s.repl.AuthorizedInternal(r) {
		http.Error(w, "unauthorized internal request", http.StatusUnauthorized)
		return
	}
	if s.faults != nil && s.faults.IsBlocked(r.Header.Get("X-Vault-Node-ID")) {
		http.Error(w, "fault injection: replica partitioned", http.StatusServiceUnavailable)
		return
	}
	const prefix = "/internal/v1/replicas/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if key == "" {
		http.Error(w, "object key is required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.repl.InternalPut(w, r, key)
	case http.MethodGet, http.MethodHead:
		s.repl.InternalGet(w, r, key)
	case http.MethodDelete:
		s.repl.InternalDelete(w, r, key)
	default:
		methodNotAllowed(w, http.MethodPut, http.MethodGet, http.MethodHead, http.MethodDelete)
	}
}

func (s *Server) replicaGC(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil || !s.repl.AuthorizedInternal(r) {
		http.Error(w, "unauthorized internal request", http.StatusUnauthorized)
		return
	}
	const prefix = "/internal/v1/replicas-gc/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if r.Method != http.MethodPost || key == "" {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	s.repl.InternalGC(w, r, key)
}

func (s *Server) faultsRoot(w http.ResponseWriter, r *http.Request) {
	if s.faults == nil || !s.faults.Enabled() {
		http.Error(w, "fault injection is disabled", http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.faults.Snapshot())
	case http.MethodPost:
		var req struct {
			Action string   `json:"action"`
			PeerID string   `json:"peer_id"`
			Peers  []string `json:"peers"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
			http.Error(w, "invalid fault request", http.StatusBadRequest)
			return
		}
		switch req.Action {
		case "partition":
			peers := append([]string(nil), req.Peers...)
			if req.PeerID != "" {
				peers = append(peers, req.PeerID)
			}
			if len(peers) == 0 {
				http.Error(w, "peer_id or peers required", http.StatusBadRequest)
				return
			}
			for _, id := range peers {
				s.faults.Block(id)
			}
			writeJSON(w, http.StatusOK, s.faults.Snapshot())
		case "clear":
			if req.PeerID != "" {
				s.faults.Unblock(req.PeerID)
			} else {
				s.faults.Clear()
			}
			writeJSON(w, http.StatusOK, s.faults.Snapshot())
		default:
			http.Error(w, "action must be partition or clear", http.StatusBadRequest)
		}
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) rebalance(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil {
		http.Error(w, "rebalance service is disabled", http.StatusNotImplemented)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, s.repl.RebalanceStats())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	stats, err := s.repl.RebalanceAll(ctx)
	if err != nil {
		s.writeReplicationError(w, r, "", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) rebalanceObject(w http.ResponseWriter, r *http.Request) {
	if s.repl == nil {
		http.Error(w, "rebalance service is disabled", http.StatusNotImplemented)
		return
	}
	const prefix = "/v1/rebalance/"
	key := strings.TrimPrefix(r.URL.Path, prefix)
	if key == "" || r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	moved, err := s.repl.RebalanceObject(ctx, key)
	if err != nil {
		s.writeReplicationError(w, r, key, err)
		return
	}
	record, err := s.repl.Metadata().Get(key)
	if err != nil {
		s.writeReplicationError(w, r, key, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"moved": moved, "record": record})
}

func (s *Server) writeReplicationError(w http.ResponseWriter, r *http.Request, key string, err error) {
	if leaderID, leaderAddress, ok := replication.LeaderOf(err); ok {
		if leaderID != "" {
			w.Header().Set("X-Vault-Leader-ID", leaderID)
		}
		if leaderAddress != "" {
			w.Header().Set("X-Vault-Leader-Address", leaderAddress)
			if r.Method == http.MethodPut || r.Method == http.MethodDelete {
				target := strings.TrimRight(leaderAddress, "/") + r.URL.RequestURI()
				w.Header().Set("Location", target)
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}
		}
		w.WriteHeader(http.StatusConflict)
		return
	}
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, storage.ErrNotFound), errors.Is(err, metadata.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, storage.ErrObjectTooLarge):
		status = http.StatusRequestEntityTooLarge
	case errors.Is(err, replication.ErrReplicationFactor):
		status = http.StatusConflict
	case errors.Is(err, replication.ErrReplicationUnavailable), errors.Is(err, replication.ErrWriteQuorumUnavailable), errors.Is(err, replication.ErrReadQuorumUnavailable):
		status = http.StatusServiceUnavailable
	case errors.Is(err, storage.ErrChecksumMismatch), errors.Is(err, storage.ErrVersionConflict):
		status = http.StatusConflict
	}
	http.Error(w, err.Error(), status)
}

func writeObjectHeaders(w http.ResponseWriter, record metadata.Record) {
	w.Header().Set("ETag", quoteETag(record.SHA256))
	w.Header().Set("X-Vault-Object-Version", fmt.Sprint(record.Version))
	w.Header().Set("X-Vault-Object-Size", fmt.Sprint(record.Size))
	w.Header().Set("X-Vault-Object-SHA256", record.SHA256)
	w.Header().Set("X-Vault-Object-Key", record.Key)
	if len(record.Replicas) > 0 {
		w.Header().Set("X-Vault-Replica-Count", fmt.Sprint(len(record.Replicas)))
		w.Header().Set("X-Vault-Replicas", strings.Join(record.Replicas, ","))
	}
}

func setObjectHeaders(w http.ResponseWriter, record metadata.Record) {
	w.Header().Set("Content-Type", record.ContentType)
	w.Header().Set("Content-Length", fmt.Sprint(record.Size))
	w.Header().Set("ETag", quoteETag(record.SHA256))
	w.Header().Set("X-Vault-Object-Version", fmt.Sprint(record.Version))
	w.Header().Set("X-Vault-Object-Size", fmt.Sprint(record.Size))
	w.Header().Set("X-Vault-Object-SHA256", record.SHA256)
	w.Header().Set("X-Vault-Object-Key", record.Key)
	w.Header().Set("Last-Modified", record.UpdatedAt.UTC().Format(http.TimeFormat))
	if len(record.Replicas) > 0 {
		w.Header().Set("X-Vault-Replicas", strings.Join(record.Replicas, ","))
	}
}

func proxyResponse(w http.ResponseWriter, resp *http.Response, includeBody bool) {
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if includeBody && resp.Body != nil {
		_, _ = io.Copy(w, resp.Body)
	}
}

func (s *Server) writeStorageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrNotFound), errors.Is(err, metadata.ErrNotFound):
		http.Error(w, "object not found", http.StatusNotFound)
	case errors.Is(err, storage.ErrInvalidObject):
		http.Error(w, "stored object is invalid", http.StatusInternalServerError)
	case errors.Is(err, replication.ErrReadQuorumUnavailable), errors.Is(err, replication.ErrReplicationUnavailable):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func quoteETag(value string) string { return `"` + value + `"` }

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r) })
}
