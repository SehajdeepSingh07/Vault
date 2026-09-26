FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/vaultd ./cmd/vaultd \
    && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/vaultctl ./cmd/vaultctl

FROM alpine:3.20
RUN addgroup -S vault && adduser -S vault -G vault
COPY --from=build /out/vaultd /usr/local/bin/vaultd
COPY --from=build /out/vaultctl /usr/local/bin/vaultctl
RUN mkdir -p /var/lib/vault && chown -R vault:vault /var/lib/vault
USER vault
EXPOSE 8080
ENV VAULT_ADDR=0.0.0.0:8080 \
    VAULT_DATA_DIR=/var/lib/vault \
    VAULT_MAX_OBJECT_SIZE=10737418240 \
    VAULT_REPLICATION_FACTOR=3 \
    VAULT_WRITE_QUORUM=2 \
    VAULT_READ_QUORUM=2 \
    VAULT_CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
ENTRYPOINT ["/usr/local/bin/vaultd"]
