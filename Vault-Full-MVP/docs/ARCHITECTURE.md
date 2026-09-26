# Vault Architecture

## Control plane

Raft replicates metadata state. Object metadata contains key, version, size, checksum, content type, timestamps, and replica locations.

## Data plane

Objects are stored as versioned local filesystem copies. PUT stages the upload, writes replicas in parallel, waits for the configured write quorum, then commits metadata through Raft.

## Reads

Reads resolve committed metadata and probe recorded replicas. A read succeeds only after the configured read quorum agrees on version, size, and checksum metadata.

## Recovery

The failure detector tracks node states. Integrity scrubbers detect corruption. The repair worker reconstructs failed or corrupt replicas from a verified healthy source. The rebalancer updates placement when the healthy node set changes.

## Frontend boundary

The browser client is not embedded in the backend. Any future frontend can use the same `/v1` API and the OpenAPI contract.
