package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nitesh/vault/internal/api"
	"github.com/nitesh/vault/internal/cluster"
	"github.com/nitesh/vault/internal/faults"
	"github.com/nitesh/vault/internal/metadata"
	"github.com/nitesh/vault/internal/raft"
	"github.com/nitesh/vault/internal/replication"
	"github.com/nitesh/vault/internal/storage"
)

func main() {
	addr := envOr("VAULT_ADDR", "127.0.0.1:8080")
	dataDir := envOr("VAULT_DATA_DIR", "./data")
	maxObjectSize := envInt64("VAULT_MAX_OBJECT_SIZE", 10*1024*1024*1024)
	nodeID := envOr("VAULT_NODE_ID", "node-1")
	nodeAddress := envOr("VAULT_NODE_ADDRESS", "http://"+addr)
	peers, err := parseNodes(envOr("VAULT_CLUSTER_NODES", ""))
	if err != nil {
		log.Fatalf("parse cluster nodes: %v", err)
	}

	faultsInjector := faults.New(envBool("VAULT_ENABLE_FAULT_INJECTION", false))

	store, err := storage.New(dataDir, maxObjectSize)
	if err != nil {
		log.Fatalf("initialize storage: %v", err)
	}
	defer store.Close()

	membership, err := cluster.New(cluster.Config{
		Self:               cluster.NodeInfo{ID: nodeID, Address: nodeAddress},
		Peers:              peers,
		HeartbeatInterval:  envDurationMillis("VAULT_HEARTBEAT_INTERVAL_MS", 500*time.Millisecond),
		SuspectAfter:       envDurationMillis("VAULT_NODE_SUSPECT_AFTER_MS", 1500*time.Millisecond),
		FailAfter:          envDurationMillis("VAULT_NODE_FAIL_AFTER_MS", 3500*time.Millisecond),
		RecoveryHeartbeats: uint64(envInt("VAULT_RECOVERY_HEARTBEATS", 2)),
		ReplicationFactor:  envInt("VAULT_REPLICATION_FACTOR", 3),
		WriteQuorum:        envInt("VAULT_WRITE_QUORUM", 2),
		ReadQuorum:         envInt("VAULT_READ_QUORUM", 2),
		Faults:             faultsInjector,
	})
	if err != nil {
		log.Fatalf("initialize cluster membership: %v", err)
	}
	if err := membership.Start(); err != nil {
		log.Fatalf("start cluster membership: %v", err)
	}
	defer membership.Stop()

	metaStore := metadata.NewStore()
	raftNode, err := raft.New(raft.Config{
		ID:          nodeID,
		Address:     nodeAddress,
		Peers:       toRaftPeers(peers),
		DataDir:     dataDir + "/raft",
		ElectionMin: envDurationMillis("VAULT_RAFT_ELECTION_MIN_MS", 900*time.Millisecond),
		ElectionMax: envDurationMillis("VAULT_RAFT_ELECTION_MAX_MS", 1500*time.Millisecond),
		Heartbeat:   envDurationMillis("VAULT_RAFT_HEARTBEAT_MS", 250*time.Millisecond),
		RPCTimeout:  envDurationMillis("VAULT_RAFT_RPC_TIMEOUT_MS", 500*time.Millisecond),
		FSM:         metaStore,
		Faults:      faultsInjector,
	})
	if err != nil {
		log.Fatalf("initialize metadata raft: %v", err)
	}
	if err := raftNode.Start(); err != nil {
		log.Fatalf("start metadata raft: %v", err)
	}
	defer raftNode.Stop()

	metaService := metadata.NewService(metaStore, raftNode)
	repl, err := replication.New(replication.Config{
		Store:             store,
		Membership:        membership,
		Metadata:          metaService,
		ReplicationFactor: envInt("VAULT_REPLICATION_FACTOR", 3),
		WriteQuorum:       envInt("VAULT_WRITE_QUORUM", 2),
		ReadQuorum:        envInt("VAULT_READ_QUORUM", 2),
		TempDir:           dataDir + "/replication-staging",
		IntegrityDir:      dataDir + "/integrity",
		InternalToken:     envOr("VAULT_INTERNAL_TOKEN", ""),
		Client:            &http.Client{Timeout: envDurationMillis("VAULT_INTERNAL_HTTP_TIMEOUT_MS", 10000*time.Millisecond)},
		Faults:            faultsInjector,
	})
	if err != nil {
		log.Fatalf("initialize replication coordinator: %v", err)
	}
	scrubCtx, stopScrubber := context.WithCancel(context.Background())
	defer stopScrubber()
	repl.StartScrubber(scrubCtx, envDurationMillis("VAULT_INTEGRITY_SCRUB_INTERVAL_MS", 60000*time.Millisecond))
	repl.StartRepairer(scrubCtx, envDurationMillis("VAULT_REPAIR_INTERVAL_MS", 2000*time.Millisecond))
	repl.StartRebalancer(scrubCtx, envDurationMillis("VAULT_REBALANCE_INTERVAL_MS", 10000*time.Millisecond))
	allowedOrigins := parseCSV(envOr("VAULT_CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000"))
	baseHandler := api.NewWithMetadataAndReplication(store, membership, metaService, repl, faultsInjector).Handler()
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           api.WithCORS(baseHandler, allowedOrigins),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		WriteTimeout:      0,
		MaxHeaderBytes:    1 << 20,
	}

	go func() {
		log.Printf("vault node=%s listening on http://%s", nodeID, addr)
		log.Printf("advertised address: %s", nodeAddress)
		log.Printf("cluster peers: %d", len(peers))
		log.Printf("data directory: %s", dataDir)
		log.Printf("max object size: %d bytes", maxObjectSize)
		log.Printf("replication factor: %d", repl.ReplicationFactor())
		log.Printf("write quorum: %d", repl.WriteQuorum())
		log.Printf("read quorum: %d", repl.ReadQuorum())
		log.Printf("integrity scrub interval: %s", envDurationMillis("VAULT_INTEGRITY_SCRUB_INTERVAL_MS", 60000*time.Millisecond))
		log.Printf("repair interval: %s", envDurationMillis("VAULT_REPAIR_INTERVAL_MS", 2000*time.Millisecond))
		log.Printf("rebalance interval: %s", envDurationMillis("VAULT_REBALANCE_INTERVAL_MS", 10000*time.Millisecond))
		log.Printf("fault injection enabled: %t", faultsInjector.Enabled())
		log.Printf("cors origins: %s", strings.Join(allowedOrigins, ","))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
		_ = httpServer.Close()
	}
}

func toRaftPeers(nodes []cluster.NodeInfo) []raft.Peer {
	result := make([]raft.Peer, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, raft.Peer{ID: node.ID, Address: node.Address})
	}
	return result
}

func parseNodes(raw string) ([]cluster.NodeInfo, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	result := make([]cluster.NodeInfo, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.IndexByte(part, '=')
		if idx <= 0 || idx == len(part)-1 {
			return nil, fmt.Errorf("invalid cluster node %q; expected id=address", part)
		}
		id := strings.TrimSpace(part[:idx])
		address := strings.TrimSpace(part[idx+1:])
		if id == "" || address == "" {
			return nil, fmt.Errorf("invalid cluster node %q", part)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate cluster node %q", id)
		}
		seen[id] = struct{}{}
		result = append(result, cluster.NodeInfo{ID: id, Address: address})
	}
	return result, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil && parsed > 0 {
			return parsed
		}
		fmt.Fprintf(os.Stderr, "invalid %s=%q; using default %d\n", key, value, fallback)
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil && parsed > 0 {
			return parsed
		}
		fmt.Fprintf(os.Stderr, "invalid %s=%q; using default %d\n", key, value, fallback)
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if value := strings.TrimSpace(strings.ToLower(os.Getenv(key))); value != "" {
		switch value {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
		fmt.Fprintf(os.Stderr, "invalid %s=%q; using default %t\n", key, value, fallback)
	}
	return fallback
}

func envDurationMillis(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil && parsed > 0 {
			return time.Duration(parsed) * time.Millisecond
		}
		fmt.Fprintf(os.Stderr, "invalid %s=%q; using default %s\n", key, value, fallback)
	}
	return fallback
}

func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		result = append(result, part)
	}
	return result
}
