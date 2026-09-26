# Vault Backend

The backend is the distributed storage engine. It exposes only versioned HTTP APIs and operator CLI commands; the browser UI is in the sibling `frontend/` directory.

## Start one local node

```bash
go run ./cmd/vaultd
```

The default API is `http://127.0.0.1:8080`.

## Test

```bash
go test ./...
go test -race ./...
go vet ./...
```

## Build

```bash
go build -o bin/vaultd ./cmd/vaultd
go build -o bin/vaultctl ./cmd/vaultctl
```

The source under `tests/final-integration/` is the multi-process chaos/integration harness used for the combined MVP verification.
