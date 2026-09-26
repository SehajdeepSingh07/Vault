package api

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

func WithCORS(next http.Handler, allowedOrigins []string) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	allowAll := false
	for _, origin := range allowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		if origin == "*" {
			allowAll = true
			continue
		}
		allowed[origin] = struct{}{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if _, ok := allowed[origin]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, PUT, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Vault-Internal-Token, X-Request-ID")
		w.Header().Set("Access-Control-Expose-Headers", "ETag, X-Vault-Object-Version, X-Vault-Object-Size, X-Vault-Object-SHA256, X-Vault-Object-Key, X-Vault-Replica-Count, X-Vault-Replicas, X-Vault-Read-Quorum, X-Vault-Read-Acks, X-Vault-Write-Quorum, X-Vault-Write-Acks, X-Vault-Leader-ID, X-Vault-Leader-Address, X-Vault-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" || len(id) > 128 {
			id = newRequestID()
		}
		w.Header().Set("X-Vault-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "vault-request"
	}
	return hex.EncodeToString(b[:])
}
