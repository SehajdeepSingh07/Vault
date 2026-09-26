package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithCORS(t *testing.T) {
	h := WithCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), []string{"http://localhost:3000"})

	req := httptest.NewRequest(http.MethodOptions, "/v1/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "GET")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("preflight status=%d", resp.Code)
	}
	if got := resp.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Fatalf("allow origin=%q", got)
	}
}

func TestRootAndVersionEndpoints(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("root status=%d", resp.StatusCode)
	}
	resp, err = http.Get(ts.URL + "/v1/version")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("version status=%d", resp.StatusCode)
	}
}
