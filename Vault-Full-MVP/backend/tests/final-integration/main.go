package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

type nodeProc struct {
	id, port, dir string
	cmd           *exec.Cmd
}
type record struct {
	Key      string   `json:"key"`
	Version  uint64   `json:"version"`
	Replicas []string `json:"replicas"`
}

type raftView struct {
	Status struct {
		Role string `json:"role"`
	} `json:"status"`
}

type rebalanceStats struct{ Runs, ObjectsScanned, ObjectsMoved, ReplicasMoved, FailedObjects uint64 }

type clusterView struct {
	Peers []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"peers"`
}

type faultView struct {
	Enabled bool     `json:"enabled"`
	Blocked []string `json:"blocked_peers"`
}

func main() {
	binary := filepath.Join(filepath.Dir(os.Args[0]), "vaultd")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	binary, _ = filepath.Abs(binary)
	root := mustTemp("vault-step10-live-")
	defer os.RemoveAll(root)
	ports := []string{"28901", "28902", "28903", "28904"}
	ids := []string{"node-1", "node-2", "node-3", "node-4"}
	clusterNodes := "node-1=http://127.0.0.1:28901,node-2=http://127.0.0.1:28902,node-3=http://127.0.0.1:28903,node-4=http://127.0.0.1:28904"
	nodes := make([]nodeProc, 4)
	for i := range nodes {
		nodes[i] = nodeProc{id: ids[i], port: ports[i], dir: filepath.Join(root, ids[i])}
		must(os.MkdirAll(nodes[i].dir, 0o750))
		start(&nodes[i], binary, clusterNodes)
	}
	defer func() {
		for i := range nodes {
			stop(&nodes[i])
		}
	}()
	for i := range nodes {
		wait(urlFor(nodes[i], "/v1/health"), 20*time.Second)
	}
	leader := waitLeaderAmong(nodes, "node-4", 20*time.Second)

	checkDashboard(leader)
	checkCLI(filepath.Join(filepath.Dir(binary), "vaultctl"), leader)

	key := findRebalanceKey([]string{"node-1", "node-2", "node-3", "node-4"}, 3)
	pre := placement(key, []string{"node-1", "node-2", "node-3"}, 3)
	full := placement(key, []string{"node-1", "node-2", "node-3", "node-4"}, 3)
	if contains(full, "node-4") == false || sameSet(pre, full) {
		panic(fmt.Sprintf("could not find useful rebalance key: pre=%v full=%v", pre, full))
	}
	fmt.Printf("REBALANCE_KEY=PASS key=%s before=%s after=%s\n", key, strings.Join(pre, ","), strings.Join(full, ","))

	stop(&nodes[3])
	waitNodeFailed(leader, "node-4", 10*time.Second)
	payload := []byte("vault step 10 final integration payload\n")
	resp := request("PUT", urlFor(leader, "/v1/objects/"+key), bytes.NewReader(payload), "text/plain", 20*time.Second)
	mustStatus(resp, http.StatusCreated)
	resp.Body.Close()
	resp = request("HEAD", urlFor(leader, "/v1/objects/"+key), nil, "", 5*time.Second)
	mustStatus(resp, http.StatusOK)
	if resp.Header.Get("X-Vault-Object-SHA256") == "" || resp.Header.Get("X-Vault-Replicas") == "" {
		resp.Body.Close()
		panic("object HEAD missing Vault headers")
	}
	resp.Body.Close()
	resp = request("GET", urlFor(leader, "/v1/objects/"+key), nil, "", 10*time.Second)
	mustStatus(resp, http.StatusOK)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != string(payload) {
		panic("object GET mismatch")
	}
	fmt.Println("OBJECT_PUT_HEAD_GET=PASS")
	rec := getRecord(leader, key)
	if contains(rec.Replicas, "node-4") {
		panic(fmt.Sprintf("offline node unexpectedly in replicas: %+v", rec))
	}
	fmt.Printf("PLACEMENT_WITH_NODE_DOWN=PASS replicas=%s\n", strings.Join(rec.Replicas, ","))

	start(&nodes[3], binary, clusterNodes)
	wait(urlFor(nodes[3], "/v1/health"), 20*time.Second)
	waitPeerHealthy(leader, "node-4", 12*time.Second)
	waitMetadataCatchup(nodes, key, rec.Version, 15*time.Second)
	leader = waitLeader(nodes, 15*time.Second)
	check := getRecord(leader, key)
	if check.Version < rec.Version {
		panic(fmt.Sprintf("leader metadata stale: %+v", check))
	}
	fmt.Printf("POST_RECOVERY_LEADER=PASS leader=%s metadata_version=%d replicas=%s\n", leader.id, check.Version, strings.Join(check.Replicas, ","))
	listResp := request("GET", urlFor(leader, "/v1/metadata"), nil, "", 5*time.Second)
	mustStatus(listResp, http.StatusOK)
	var active []record
	must(json.NewDecoder(listResp.Body).Decode(&active))
	listResp.Body.Close()
	fmt.Printf("ACTIVE_METADATA_COUNT=%d\n", len(active))

	waitReplicaSet(nodes, key, full, 20*time.Second)
	leader = waitLeader(nodes, 15*time.Second)
	final := getRecord(leader, key)
	if !sameSet(final.Replicas, full) {
		panic(fmt.Sprintf("background rebalance target mismatch: got=%v want=%v", final.Replicas, full))
	}
	fmt.Printf("BACKGROUND_REBALANCE=PASS replicas=%s\n", strings.Join(final.Replicas, ","))

	resp = request("POST", urlFor(leader, "/v1/rebalance/"+key), nil, "", 60*time.Second)
	mustStatus(resp, http.StatusOK)
	var result struct {
		Moved  int    `json:"moved"`
		Record record `json:"record"`
	}
	must(json.NewDecoder(resp.Body).Decode(&result))
	resp.Body.Close()
	if result.Moved != 0 || !sameSet(result.Record.Replicas, full) {
		panic(fmt.Sprintf("manual rebalance not idempotent: %+v", result))
	}
	fmt.Println("MANUAL_REBALANCE_IDEMPOTENT=PASS")

	// Partition an elected leader away from the other three nodes.
	leader = waitLeader(nodes, 15*time.Second)
	isolated := leader
	for i := range nodes {
		if nodes[i].id == isolated.id {
			continue
		}
		partition(isolated, nodes[i].id)
		partition(nodes[i], isolated.id)
	}
	fv := getFaults(isolated)
	if len(fv.Blocked) != 3 {
		panic(fmt.Sprintf("fault injection did not install partition: %+v", fv))
	}
	fmt.Printf("PARTITION_INJECTED=PASS isolated=%s peers=%s\n", isolated.id, strings.Join(fv.Blocked, ","))

	majority := waitLeaderAmong(nodes, isolated.id, 12*time.Second)
	if majority.id == isolated.id {
		panic("isolated node remained majority leader")
	}
	fmt.Printf("MAJORITY_LEADER=PASS leader=%s\n", majority.id)

	resp = request("PUT", urlFor(majority, "/v1/objects/partition/majority.txt"), bytes.NewBufferString("majority-write\n"), "text/plain", 20*time.Second)
	mustStatus(resp, http.StatusCreated)
	resp.Body.Close()
	fmt.Println("MAJORITY_WRITE=PASS")

	resp = request("PUT", urlFor(isolated, "/v1/objects/partition/isolated.txt"), bytes.NewBufferString("isolated-write\n"), "text/plain", 7*time.Second)
	if resp.StatusCode == http.StatusCreated {
		resp.Body.Close()
		panic("isolated partition unexpectedly committed a write")
	}
	resp.Body.Close()
	fmt.Printf("ISOLATED_WRITE_REJECTED=PASS status=%d\n", resp.StatusCode)

	for i := range nodes {
		clearFault(nodes[i])
	}
	wait(urlFor(leader, "/v1/health"), 10*time.Second)
	for i := range nodes {
		wait(urlFor(nodes[i], "/v1/health"), 20*time.Second)
	}
	waitClusterRecovered(nodes, 15*time.Second)
	resp = request("GET", urlFor(majority, "/v1/objects/partition/majority.txt"), nil, "", 10*time.Second)
	mustStatus(resp, http.StatusOK)
	gotMajority, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(gotMajority) != "majority-write\n" {
		panic("majority object data mismatch after partition recovery")
	}
	fmt.Println("PARTITION_RECOVERY=PASS")
	deleteKey := "step10/final/delete-me.txt"
	resp = request("PUT", urlFor(majority, "/v1/objects/"+deleteKey), bytes.NewBufferString("delete-me\n"), "text/plain", 15*time.Second)
	mustStatus(resp, http.StatusCreated)
	resp.Body.Close()
	resp = request("DELETE", urlFor(majority, "/v1/objects/"+deleteKey), nil, "", 15*time.Second)
	mustStatus(resp, http.StatusNoContent)
	resp.Body.Close()
	resp = request("HEAD", urlFor(majority, "/v1/objects/"+deleteKey), nil, "", 5*time.Second)
	if resp.StatusCode != http.StatusNotFound {
		resp.Body.Close()
		panic(fmt.Sprintf("delete smoke expected 404, got %d", resp.StatusCode))
	}
	resp.Body.Close()
	fmt.Println("OBJECT_DELETE=PASS")
	fmt.Println("STEP 10 LIVE FINAL INTEGRATION TEST PASSED")
}

func checkDashboard(n nodeProc) {
	resp := request("GET", urlFor(n, "/"), nil, "", 5*time.Second)
	mustStatus(resp, http.StatusOK)
	var root map[string]any
	must(json.NewDecoder(resp.Body).Decode(&root))
	resp.Body.Close()
	if root["service"] != "vault" || root["api"] != "/v1" {
		panic(fmt.Sprintf("root API contract mismatch: %+v", root))
	}
	resp = request("GET", urlFor(n, "/v1/version"), nil, "", 5*time.Second)
	mustStatus(resp, http.StatusOK)
	var version map[string]any
	must(json.NewDecoder(resp.Body).Decode(&version))
	resp.Body.Close()
	if version["api_version"] != "v1" {
		panic(fmt.Sprintf("version contract mismatch: %+v", version))
	}
	preflight, err := http.NewRequest(http.MethodOptions, urlFor(n, "/v1/health"), nil)
	if err != nil {
		panic(err)
	}
	preflight.Header.Set("Origin", "http://localhost:3000")
	preflightResp, err := (&http.Client{Timeout: 5 * time.Second}).Do(preflight)
	if err != nil {
		panic(err)
	}
	preflightResp.Body.Close()
	if preflightResp.StatusCode != http.StatusNoContent || preflightResp.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		panic(fmt.Sprintf("CORS preflight mismatch: status=%d origin=%q", preflightResp.StatusCode, preflightResp.Header.Get("Access-Control-Allow-Origin")))
	}
	fmt.Println("BACKEND_API_CONTRACT=PASS")
}

func checkCLI(binary string, n nodeProc) {
	if runtime.GOOS != "windows" {
		binary, _ = filepath.Abs(binary)
		if _, err := os.Stat(binary); err != nil {
			panic("vaultctl binary missing: " + binary)
		}
	}
	cmd := exec.Command(binary, "status")
	cmd.Env = append(os.Environ(), "VAULT_URL="+urlFor(n, ""))
	out, err := cmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("vaultctl status failed: %v\n%s", err, out))
	}
	if !bytes.Contains(out, []byte("HTTP 200")) {
		panic("vaultctl status output missing HTTP 200")
	}
	fmt.Println("CLI_STATUS=PASS")
}

func placement(key string, ids []string, rf int) []string {
	type scored struct{ id, score string }
	xs := make([]scored, 0, len(ids))
	for _, id := range ids {
		h := sha256.Sum256([]byte(key + "\x00" + id))
		xs = append(xs, scored{id, hex.EncodeToString(h[:])})
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i].score < xs[j].score })
	if rf > len(xs) {
		rf = len(xs)
	}
	out := make([]string, rf)
	for i := 0; i < rf; i++ {
		out[i] = xs[i].id
	}
	sort.Strings(out)
	return out
}
func findRebalanceKey(ids []string, rf int) string {
	for i := 0; i < 10000; i++ {
		k := "step10/rebalance/" + strconv.Itoa(i)
		a := placement(k, ids[:3], rf)
		b := placement(k, ids, rf)
		if !sameSet(a, b) && contains(b, "node-4") {
			return k
		}
	}
	panic("no rebalance key found")
}
func partition(n nodeProc, peer string) {
	body := fmt.Sprintf(`{"action":"partition","peer_id":%q}`, peer)
	r := request("POST", urlFor(n, "/v1/faults"), strings.NewReader(body), "application/json", 5*time.Second)
	mustStatus(r, 200)
	r.Body.Close()
}
func clearFault(n nodeProc) {
	r := request("POST", urlFor(n, "/v1/faults"), strings.NewReader(`{"action":"clear"}`), "application/json", 5*time.Second)
	mustStatus(r, 200)
	r.Body.Close()
}
func getFaults(n nodeProc) faultView {
	r := request("GET", urlFor(n, "/v1/faults"), nil, "", 5*time.Second)
	mustStatus(r, 200)
	var v faultView
	must(json.NewDecoder(r.Body).Decode(&v))
	r.Body.Close()
	return v
}
func getRecord(n nodeProc, key string) record {
	r := request("GET", urlFor(n, "/v1/metadata/"+key), nil, "", 5*time.Second)
	mustStatus(r, 200)
	var v record
	must(json.NewDecoder(r.Body).Decode(&v))
	r.Body.Close()
	return v
}

func waitReplicaSet(nodes []nodeProc, key string, want []string, t time.Duration) {
	d := time.Now().Add(t)
	for time.Now().Before(d) {
		for _, n := range nodes {
			r, err := (&http.Client{Timeout: 3 * time.Second}).Get(urlFor(n, "/v1/metadata/"+key))
			if err != nil {
				continue
			}
			if r.StatusCode != http.StatusOK {
				r.Body.Close()
				continue
			}
			var x record
			if json.NewDecoder(r.Body).Decode(&x) == nil && sameSet(x.Replicas, want) {
				r.Body.Close()
				return
			}
			r.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	panic("background rebalancing timeout: " + key)
}

func waitMetadataCatchup(nodes []nodeProc, key string, version uint64, t time.Duration) {
	d := time.Now().Add(t)
	for time.Now().Before(d) {
		ok := true
		for _, n := range nodes {
			r, err := (&http.Client{Timeout: 3 * time.Second}).Get(urlFor(n, "/v1/metadata/"+key))
			if err != nil {
				ok = false
				break
			}
			if r.StatusCode != http.StatusOK {
				r.Body.Close()
				ok = false
				break
			}
			var x record
			if json.NewDecoder(r.Body).Decode(&x) != nil {
				ok = false
			}
			r.Body.Close()
			if x.Version < version {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	panic("metadata catch-up timeout: " + key)
}

func waitPeerHealthy(n nodeProc, id string, t time.Duration) {
	d := time.Now().Add(t)
	for time.Now().Before(d) {
		r := request("GET", urlFor(n, "/v1/cluster"), nil, "", 3*time.Second)
		if r.StatusCode == 200 {
			var v clusterView
			if json.NewDecoder(r.Body).Decode(&v) == nil {
				r.Body.Close()
				for _, p := range v.Peers {
					if p.ID == id && p.Status == "healthy" {
						return
					}
				}
			} else {
				r.Body.Close()
			}
		} else {
			r.Body.Close()
		}
		time.Sleep(150 * time.Millisecond)
	}
	panic("peer recovery timeout: " + id)
}
func waitNodeFailed(n nodeProc, id string, t time.Duration) {
	d := time.Now().Add(t)
	for time.Now().Before(d) {
		r, err := (&http.Client{Timeout: 3 * time.Second}).Get(urlFor(n, "/v1/cluster"))
		if err == nil {
			if r.StatusCode == 200 {
				var v clusterView
				_ = json.NewDecoder(r.Body).Decode(&v)
				for _, p := range v.Peers {
					if p.ID == id && p.Status == "failed" {
						r.Body.Close()
						return
					}
				}
			}
			r.Body.Close()
		}
		time.Sleep(150 * time.Millisecond)
	}
	panic("node failed timeout: " + id)
}
func waitClusterRecovered(nodes []nodeProc, t time.Duration) {
	d := time.Now().Add(t)
	for time.Now().Before(d) {
		ok := true
		for _, n := range nodes {
			r := request("GET", urlFor(n, "/v1/cluster"), nil, "", 2*time.Second)
			if r.StatusCode != 200 {
				ok = false
				if r != nil {
					r.Body.Close()
				}
				break
			}
			var v clusterView
			if json.NewDecoder(r.Body).Decode(&v) != nil {
				ok = false
			}
			r.Body.Close()
			if len(v.Peers) != 3 {
				ok = false
			}
			for _, p := range v.Peers {
				if p.Status != "healthy" {
					ok = false
				}
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	panic("cluster did not recover")
}
func waitLeader(nodes []nodeProc, t time.Duration) nodeProc { return waitLeaderAmong(nodes, "", t) }
func waitLeaderAmong(nodes []nodeProc, skip string, t time.Duration) nodeProc {
	d := time.Now().Add(t)
	for time.Now().Before(d) {
		for _, n := range nodes {
			if n.id == skip || n.cmd == nil {
				continue
			}
			r, e := http.Get(urlFor(n, "/v1/raft"))
			if e != nil {
				continue
			}
			var v raftView
			_ = json.NewDecoder(r.Body).Decode(&v)
			r.Body.Close()
			if v.Status.Role == "leader" {
				return n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("leader timeout")
}
func start(n *nodeProc, binary, clusterNodes string) {
	cmd := exec.Command(binary)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.Env = append(os.Environ(), "VAULT_NODE_ID="+n.id, "VAULT_ADDR=127.0.0.1:"+n.port, "VAULT_NODE_ADDRESS=http://127.0.0.1:"+n.port, "VAULT_CLUSTER_NODES="+clusterNodes, "VAULT_DATA_DIR="+n.dir, "VAULT_REPLICATION_FACTOR=3", "VAULT_WRITE_QUORUM=2", "VAULT_READ_QUORUM=2", "VAULT_RAFT_ELECTION_MIN_MS=500", "VAULT_RAFT_ELECTION_MAX_MS=800", "VAULT_RAFT_HEARTBEAT_MS=150", "VAULT_HEARTBEAT_INTERVAL_MS=150", "VAULT_NODE_SUSPECT_AFTER_MS=350", "VAULT_NODE_FAIL_AFTER_MS=900", "VAULT_RECOVERY_HEARTBEATS=3", "VAULT_INTERNAL_HTTP_TIMEOUT_MS=2500", "VAULT_INTEGRITY_SCRUB_INTERVAL_MS=0", "VAULT_REPAIR_INTERVAL_MS=0", "VAULT_REBALANCE_INTERVAL_MS=500", "VAULT_ENABLE_FAULT_INJECTION=true")
	must(cmd.Start())
	n.cmd = cmd
}
func stop(n *nodeProc) {
	if n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Kill()
	_, _ = n.cmd.Process.Wait()
	n.cmd = nil
}
func wait(u string, t time.Duration) {
	d := time.Now().Add(t)
	for time.Now().Before(d) {
		r, e := http.Get(u)
		if e == nil && r.StatusCode == 200 {
			r.Body.Close()
			return
		}
		if r != nil {
			r.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("endpoint timeout: " + u)
}
func request(method, u string, body io.Reader, ct string, t time.Duration) *http.Response {
	c := &http.Client{Timeout: t}
	req, e := http.NewRequest(method, u, body)
	must(e)
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	r, e := c.Do(req)
	must(e)
	return r
}
func mustStatus(r *http.Response, want int) {
	if r.StatusCode != want {
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		panic(fmt.Sprintf("HTTP %d want %d body=%q", r.StatusCode, want, b))
	}
}
func urlFor(n nodeProc, p string) string { return "http://127.0.0.1:" + n.port + p }
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func mustTemp(prefix string) string { p, e := os.MkdirTemp("", prefix); must(e); return p }
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
func sameSet(a, b []string) bool {
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	if len(aa) != len(bb) {
		return false
	}
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
