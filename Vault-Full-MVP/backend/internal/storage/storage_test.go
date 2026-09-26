package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	store, err := New(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestPutGetRoundTrip(t *testing.T) {
	store := newTestStore(t)
	input := []byte("vault step one")

	meta, err := store.Put("hello/world.txt", "text/plain", bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Version != 1 || meta.Size != int64(len(input)) {
		t.Fatalf("unexpected metadata: %+v", meta)
	}

	file, got, offset, err := store.Open("hello/world.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.NewSectionReader(file, offset, got.Size))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, input) {
		t.Fatalf("data mismatch: got %q want %q", data, input)
	}

	h := sha256.Sum256(input)
	if got.SHA256 != hex.EncodeToString(h[:]) {
		t.Fatalf("checksum mismatch: got %s", got.SHA256)
	}
}

func TestOverwriteIsVersioned(t *testing.T) {
	store := newTestStore(t)
	first, err := store.Put("same", "text/plain", bytes.NewBufferString("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Put("same", "text/plain", bytes.NewBufferString("second"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != 1 || second.Version != 2 {
		t.Fatalf("unexpected versions: %d %d", first.Version, second.Version)
	}
}

func TestConcurrentSameKeyPutsAreSerialized(t *testing.T) {
	store := newTestStore(t)
	const count = 25

	var wg sync.WaitGroup
	errCh := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Put("same-key", "text/plain", bytes.NewBufferString(string(rune('a'+i))))
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	meta, err := store.Metadata("same-key")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Version != count {
		t.Fatalf("final version=%d want=%d", meta.Version, count)
	}
}

func TestDelete(t *testing.T) {
	store := newTestStore(t)
	_, err := store.Put("delete-me", "application/octet-stream", bytes.NewBufferString("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("delete-me"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Metadata("delete-me"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestInvalidObjectDetected(t *testing.T) {
	store := newTestStore(t)
	key := "broken"
	path := store.objectPath(key)
	if err := os.WriteFile(path, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Metadata(key); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("expected ErrInvalidObject, got %v", err)
	}
}

func TestObjectSizeLimit(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = store.Put("too-large", "application/octet-stream", bytes.NewBufferString("12345"))
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("expected ErrObjectTooLarge, got %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "objects", ".upload-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files leaked: %v", matches)
	}
}
