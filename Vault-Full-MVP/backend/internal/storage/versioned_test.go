package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"
)

func TestPutVersionIsCopyOnWriteAndIdempotent(t *testing.T) {
	store := newTestStore(t)
	payload1 := []byte("version one")
	h1 := sha256.Sum256(payload1)
	m1, created, err := store.PutVersion("same", "text/plain", 1, int64(len(payload1)), hex.EncodeToString(h1[:]), bytes.NewReader(payload1))
	if err != nil || !created || m1.Version != 1 {
		t.Fatalf("first put: meta=%+v created=%v err=%v", m1, created, err)
	}
	payload2 := []byte("version two")
	h2 := sha256.Sum256(payload2)
	m2, created, err := store.PutVersion("same", "text/plain", 2, int64(len(payload2)), hex.EncodeToString(h2[:]), bytes.NewReader(payload2))
	if err != nil || !created || m2.Version != 2 {
		t.Fatalf("second put: meta=%+v created=%v err=%v", m2, created, err)
	}
	again, created, err := store.PutVersion("same", "text/plain", 2, int64(len(payload2)), hex.EncodeToString(h2[:]), bytes.NewReader(payload2))
	if err != nil || created || again.SHA256 != m2.SHA256 {
		t.Fatalf("idempotent put: meta=%+v created=%v err=%v", again, created, err)
	}
	file, meta, offset, err := store.OpenVersion("same", 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(io.NewSectionReader(file, offset, meta.Size))
	file.Close()
	if err != nil || !bytes.Equal(got, payload1) {
		t.Fatalf("version 1 mismatch: %q err=%v", got, err)
	}
	_, _, _, err = store.OpenVersion("same", 99)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing version error=%v", err)
	}
}

func TestPutVersionRejectsChecksumMismatchWithoutCreatingFile(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("payload")
	bad := stringsRepeat("0", 64)
	_, _, err := store.PutVersion("broken", "text/plain", 1, int64(len(payload)), bad, bytes.NewReader(payload))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("error=%v", err)
	}
	if _, err := store.MetadataVersion("broken", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unexpected version after failed write: %v", err)
	}
}

func TestDeleteVersionsBeforeKeepsCurrent(t *testing.T) {
	store := newTestStore(t)
	for version := uint64(1); version <= 3; version++ {
		payload := []byte("v" + string(rune('0'+version)))
		h := sha256.Sum256(payload)
		if _, _, err := store.PutVersion("gc", "text/plain", version, int64(len(payload)), hex.EncodeToString(h[:]), bytes.NewReader(payload)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteVersionsBefore("gc", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MetadataVersion("gc", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("v1 still exists: %v", err)
	}
	if _, err := store.MetadataVersion("gc", 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("v2 still exists: %v", err)
	}
	if _, err := store.MetadataVersion("gc", 3); err != nil {
		t.Fatalf("v3 missing: %v", err)
	}
}

func stringsRepeat(v string, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = v[0]
	}
	return string(out)
}

func TestVerifyVersionDetectsCorruption(t *testing.T) {
	store := newTestStore(t)
	input := []byte("abcdef")
	h := sha256.Sum256(input)
	sha := hex.EncodeToString(h[:])
	if _, _, err := store.PutVersion("integrity.txt", "text/plain", 1, int64(len(input)), sha, bytes.NewReader(input)); err != nil {
		t.Fatal(err)
	}
	result, err := store.VerifyVersion("integrity.txt", 1)
	if err != nil || !result.Valid {
		t.Fatalf("expected valid object: result=%+v err=%v", result, err)
	}
	file, _, offset, err := store.OpenVersion("integrity.txt", 1)
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	_ = file.Close()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("Z"), offset); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	_ = f.Close()
	result, err = store.VerifyVersion("integrity.txt", 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || result.ActualSHA256 == result.ExpectedSHA256 {
		t.Fatalf("expected corruption: %+v", result)
	}
}
