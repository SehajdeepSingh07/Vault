package replication

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestQuarantinePersistence(t *testing.T) {
	dir := t.TempDir()
	c := &Coordinator{integrityDir: dir, quarantined: make(map[string]QuarantineRecord)}
	q := QuarantineRecord{
		Key:            "objects/a",
		Version:        7,
		ExpectedSize:   123,
		ExpectedSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ActualSize:     123,
		ActualSHA256:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Reason:         "checksum mismatch",
		DetectedAt:     time.Now().UTC(),
	}
	if err := c.markQuarantined(q); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "quarantine.json")); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadQuarantines(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := loaded[quarantineKey(q.Key, q.Version)]
	if !ok || got.ActualSHA256 != q.ActualSHA256 || got.Reason != q.Reason {
		t.Fatalf("loaded quarantine mismatch: %+v", got)
	}
}
