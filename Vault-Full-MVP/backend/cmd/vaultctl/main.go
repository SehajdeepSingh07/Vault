package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]
	baseURL := envOr("VAULT_URL", "http://127.0.0.1:8080")
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	switch args[0] {
	case "health":
		get(baseURL + "/v1/health")
	case "node":
		get(baseURL + "/v1/node")
	case "cluster":
		get(baseURL + "/v1/cluster")
	case "raft":
		raftCommand(baseURL, args[1:])
	case "integrity":
		integrityCommand(baseURL, args[1:])
	case "repair":
		repairCommand(baseURL, args[1:])
	case "rebalance":
		rebalanceCommand(baseURL, args[1:])
	case "fault":
		faultCommand(baseURL, args[1:])
	case "status":
		statusCommand(baseURL)
	case "metadata":
		metadataCommand(baseURL, args[1:])
	case "put":
		if len(args) != 3 {
			fatalf("put requires <key> <file>")
		}
		putObject(baseURL, args[1], args[2])
	case "get":
		if len(args) != 3 {
			fatalf("get requires <key> <file>")
		}
		getObject(baseURL, args[1], args[2])
	case "head":
		if len(args) != 2 {
			fatalf("head requires <key>")
		}
		headObject(baseURL, args[1])
	case "delete":
		if len(args) != 2 {
			fatalf("delete requires <key>")
		}
		deleteObject(baseURL, args[1])
	default:
		usage()
		os.Exit(2)
	}
}

func raftCommand(baseURL string, args []string) {
	if len(args) != 1 || args[0] != "status" {
		fatalf("raft status")
	}
	get(baseURL + "/v1/raft")
}

func integrityCommand(baseURL string, args []string) {
	if len(args) == 0 {
		get(baseURL + "/v1/integrity")
		return
	}
	if len(args) != 2 || (args[0] != "check" && args[0] != "scrub") {
		fatalf("integrity check|scrub <key>")
	}
	method := http.MethodPost
	if args[0] == "check" {
		method = http.MethodGet
	}
	request(method, baseURL+"/v1/integrity/"+args[1], nil, "")
}

func repairCommand(baseURL string, args []string) {
	if len(args) == 0 || (len(args) == 1 && args[0] == "status") {
		get(baseURL + "/v1/repair")
		return
	}
	if len(args) == 2 && args[0] == "run" {
		request(http.MethodPost, baseURL+"/v1/repair/"+args[1], nil, "")
		return
	}
	fatalf("repair status|run <key>")
}

func rebalanceCommand(baseURL string, args []string) {
	if len(args) == 0 || (len(args) == 1 && args[0] == "status") {
		get(baseURL + "/v1/rebalance")
		return
	}
	if len(args) == 1 && args[0] == "run" {
		request(http.MethodPost, baseURL+"/v1/rebalance", nil, "")
		return
	}
	if len(args) == 2 && args[0] == "object" {
		request(http.MethodPost, baseURL+"/v1/rebalance/"+args[1], nil, "")
		return
	}
	fatalf("rebalance status|run|object <key>")
}

func faultCommand(baseURL string, args []string) {
	if len(args) == 0 || args[0] == "status" {
		get(baseURL + "/v1/faults")
		return
	}
	if args[0] == "partition" && len(args) == 2 {
		payload := []byte(fmt.Sprintf(`{"action":"partition","peer_id":%q}`, args[1]))
		request(http.MethodPost, baseURL+"/v1/faults", bytes.NewReader(payload), "application/json")
		return
	}
	if args[0] == "clear" && len(args) == 1 {
		payload := []byte(`{"action":"clear"}`)
		request(http.MethodPost, baseURL+"/v1/faults", bytes.NewReader(payload), "application/json")
		return
	}
	fatalf("fault status|partition <node-id>|clear")
}

func statusCommand(baseURL string) {
	get(baseURL + "/v1/health")
	get(baseURL + "/v1/cluster")
	get(baseURL + "/v1/raft")
}

func metadataCommand(baseURL string, args []string) {
	if len(args) < 2 {
		fatalf("metadata get|delete or metadata put <key> <size> <sha256> [content-type]")
	}
	switch args[0] {
	case "get":
		if len(args) != 2 {
			fatalf("metadata get <key>")
		}
		get(baseURL + "/v1/metadata/" + args[1])
	case "delete":
		if len(args) != 2 {
			fatalf("metadata delete <key>")
		}
		request(http.MethodDelete, baseURL+"/v1/metadata/"+args[1], nil, "")
	case "put":
		if len(args) < 4 || len(args) > 5 {
			fatalf("metadata put <key> <size> <sha256> [content-type]")
		}
		size, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			fatalf("invalid size: %v", err)
		}
		contentType := "application/octet-stream"
		if len(args) == 5 {
			contentType = args[4]
		}
		payload, _ := json.Marshal(map[string]any{"size": size, "sha256": args[3], "content_type": contentType})
		request(http.MethodPut, baseURL+"/v1/metadata/"+args[1], bytes.NewReader(payload), "application/json")
	default:
		fatalf("unknown metadata command")
	}
}

func get(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("HTTP %d\n%s", resp.StatusCode, body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		os.Exit(1)
	}
}

func request(method, url string, body io.Reader, contentType string) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		fatalf("request build failed: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	fmt.Printf("HTTP %d\n%s", resp.StatusCode, data)
	if leader := resp.Header.Get("X-Vault-Leader-Address"); leader != "" {
		fmt.Printf("leader=%s\n", leader)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		os.Exit(1)
	}
}

func putObject(baseURL, key, path string) {
	file, err := os.Open(path)
	if err != nil {
		fatalf("open file: %v", err)
	}
	defer file.Close()
	request(http.MethodPut, baseURL+"/v1/objects/"+key, file, "application/octet-stream")
}

func getObject(baseURL, key, path string) {
	resp, err := http.Get(baseURL + "/v1/objects/" + key)
	if err != nil {
		fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fatalf("HTTP %d: %s", resp.StatusCode, body)
	}
	file, err := os.Create(path)
	if err != nil {
		fatalf("create file: %v", err)
	}
	defer file.Close()
	if _, err := io.Copy(file, resp.Body); err != nil {
		fatalf("write file: %v", err)
	}
	fmt.Println("downloaded", path)
}

func headObject(baseURL, key string) {
	resp, err := http.Head(baseURL + "/v1/objects/" + key)
	if err != nil {
		fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	fmt.Printf("HTTP %d\n", resp.StatusCode)
	for _, name := range []string{"Content-Length", "ETag", "X-Vault-Object-Version", "X-Vault-Object-SHA256", "X-Vault-Replica-Count", "X-Vault-Write-Quorum", "X-Vault-Read-Quorum", "X-Vault-Read-Acks"} {
		if value := resp.Header.Get(name); value != "" {
			fmt.Printf("%s: %s\n", name, value)
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		os.Exit(1)
	}
}
func deleteObject(baseURL, key string) {
	request(http.MethodDelete, baseURL+"/v1/objects/"+key, nil, "")
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func fatalf(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
func usage() {
	fmt.Println(`vaultctl status|health|node|cluster|raft status|integrity [check|scrub <key>]|repair [status|run <key>]|rebalance status|run|object <key>|fault status|partition <node-id>|clear|metadata get|put|delete|put/get/head/delete <args>`)
}
