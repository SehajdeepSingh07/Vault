# Vault — Fault-Tolerant Distributed Object Storage

Vault is a Go-based distributed object-storage MVP assembled from the ten implementation stages we built. The backend is API-first; the included frontend is a separate client and can be replaced later without changing the storage engine.

## What is included

- Distributed object storage over multiple independent nodes
- Raft metadata consensus
- Configurable replication factor, read quorum, and write quorum
- Concurrent reads/writes with per-object serialization
- Node heartbeats and failure states: UNKNOWN, HEALTHY, SUSPECT, FAILED, RECOVERING
- Versioned immutable object copies
- SHA-256 integrity verification and background scrubbing
- Replica quarantine and automatic repair
- Network-partition fault injection
- Background and manual rebalancing
- CLI for operators
- Standalone browser frontend
- Docker Compose demo
- Cumulative unit, race, static, cross-platform, and live multi-process tests

## Architecture

```text
                     +----------------------+
                     |  Standalone Frontend |
                     |  HTML / CSS / JS     |
                     +----------+-----------+
                                |
                            HTTP / REST
                                |
                     +----------v-----------+
                     |    Vault Backend      |
                     |   public /v1 API     |
                     +----------+-----------+
                                |
            +-------------------+-------------------+
            |                   |                   |
        +---v---+           +---v---+           +---v---+
        |Node 1 |<--Raft--->|Node 2 |<--Raft--->|Node 3 |
        |storage|           |storage|           |storage|
        +---+---+           +---+---+           +---+---+
            |                   |                   |
            +--------- replication / repair -------+
                                |
                         local filesystems
```

## Directory layout

```text
Vault-Full-MVP/
├── backend/       # Go distributed storage engine + API + CLI
├── frontend/      # Independent browser client
├── docs/          # API contract and architecture notes
├── artifacts/     # Built binaries / test helpers
└── docker-compose.yml
```

The frontend does not import backend source code. It talks only to the versioned HTTP API documented in `docs/openapi.yaml`.

## Backend — local run

From `backend/`:

```bash
go test ./...
go run ./cmd/vaultd
```

Default public API:

```text
http://127.0.0.1:8080
```

Important environment variables:

```text
VAULT_ADDR
VAULT_NODE_ID
VAULT_NODE_ADDRESS
VAULT_DATA_DIR
VAULT_CLUSTER_NODES
VAULT_REPLICATION_FACTOR
VAULT_WRITE_QUORUM
VAULT_READ_QUORUM
VAULT_INTERNAL_TOKEN
VAULT_CORS_ORIGINS
VAULT_ENABLE_FAULT_INJECTION
VAULT_INTEGRITY_SCRUB_INTERVAL_MS
VAULT_REPAIR_INTERVAL_MS
VAULT_REBALANCE_INTERVAL_MS
```

## Frontend — local run

From `frontend/`:

```bash
python3 -m http.server 3000
```

Windows:

```powershell
python -m http.server 3000
```

Open:

```text
http://127.0.0.1:3000
```

Set the backend URL in `frontend/config.js`. A different future frontend can ignore this directory entirely and use the same API contract.

## CLI

Build from `backend/`:

```bash
go build -o bin/vaultctl ./cmd/vaultctl
```

Examples:

```bash
vaultctl status
vaultctl health
vaultctl cluster
vaultctl raft status
vaultctl put examples/demo.txt examples/demo.txt
vaultctl head examples/demo.txt
vaultctl get examples/demo.txt /tmp/demo.txt
vaultctl delete examples/demo.txt
vaultctl integrity
vaultctl integrity check examples/demo.txt
vaultctl repair status
vaultctl repair run examples/demo.txt
vaultctl rebalance status
vaultctl rebalance run
vaultctl fault status
vaultctl fault partition node-2
vaultctl fault clear
```

## Docker demo

```bash
docker compose up --build
```

The browser frontend is exposed on:

```text
http://127.0.0.1:3000
```

The backend nodes are exposed on:

```text
http://127.0.0.1:8081
http://127.0.0.1:8082
http://127.0.0.1:8083
```

Change the example internal token before using this outside a local/demo environment.

## Validation

The project was built incrementally, and the combined package retains the complete behavior from Steps 1–10. The final test suite includes:

```bash
cd backend
go test ./...
go test -race ./...
go vet ./...
```

The live integration runner starts multiple independent Vault processes and exercises replication, failure detection, recovery, quorum behavior, integrity, repair, rebalancing, and partition handling.

## Engineering boundary

No software can honestly be guaranteed to “never fail in the real world.” This package is designed around fail-safe behavior, deterministic state transitions, checksums, quorum protection, recovery, and repeatable fault testing. Production deployment would still require security hardening, encrypted transport, authentication/authorization, observability, backups, operational limits, formal disaster-recovery procedures, and much larger-scale soak/chaos testing.
