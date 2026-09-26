package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nitesh/vault/internal/cluster"
	"github.com/nitesh/vault/internal/storage"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	store, err := storage.New(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return httptest.NewServer(New(store).Handler())
}

func newClusterTestServer(t *testing.T) (*httptest.Server, *cluster.Membership) {
	t.Helper()
	store, err := storage.New(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	membership, err := cluster.New(cluster.Config{Self: cluster.NodeInfo{ID: "node-1", Address: "http://127.0.0.1:19001"}, Peers: []cluster.NodeInfo{{ID: "node-2", Address: "http://127.0.0.1:19002"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(membership.Stop)
	return httptest.NewServer(New(store, membership).Handler()), membership
}

func TestHTTPObjectLifecycle(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	payload := []byte("hello vault")
	request, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/objects/folder/hello.txt", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT status=%d", resp.StatusCode)
	}
	resp.Body.Close()
	resp, err = http.Head(ts.URL + "/v1/objects/folder/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Vault-Object-Version"); got != "1" {
		t.Fatalf("version=%s", got)
	}
	if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(payload)) {
		t.Fatalf("content-length=%s", got)
	}
	resp.Body.Close()
	resp, err = http.Get(ts.URL + "/v1/objects/folder/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("GET status=%d body=%q", resp.StatusCode, body)
	}
	deleteReq, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/objects/folder/hello.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status=%d", resp.StatusCode)
	}
}

func TestHTTPConcurrentClients(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	const clients = 50
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "concurrent/object-" + strconv.Itoa(i)
			payload := bytes.Repeat([]byte{byte(i)}, 64*1024)
			request, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/objects/"+key, bytes.NewReader(payload))
			if err != nil {
				errs <- err
				return
			}
			request.Header.Set("Content-Type", "application/octet-stream")
			resp, err := http.DefaultClient.Do(request)
			if err != nil {
				errs <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				errs <- &statusError{status: resp.StatusCode}
				return
			}
			resp, err = http.Get(ts.URL + "/v1/objects/" + key)
			if err != nil {
				errs <- err
				return
			}
			got, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				errs <- readErr
				return
			}
			if resp.StatusCode != http.StatusOK || !bytes.Equal(got, payload) {
				errs <- &statusError{status: resp.StatusCode}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClusterEndpoints(t *testing.T) {
	ts, membership := newClusterTestServer(t)
	defer ts.Close()
	if err := membership.ReceiveHeartbeat(cluster.HeartbeatRequest{From: cluster.NodeInfo{ID: "node-2", Address: "http://127.0.0.1:19002"}, Protocol: 1, SentAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(ts.URL + "/v1/cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cluster status=%d", resp.StatusCode)
	}
	var snapshot cluster.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Self.ID != "node-1" || len(snapshot.Peers) != 1 || snapshot.Peers[0].Status != cluster.StatusHealthy {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

type statusError struct{ status int }

func (e *statusError) Error() string { return "unexpected status: " + strconv.Itoa(e.status) }
