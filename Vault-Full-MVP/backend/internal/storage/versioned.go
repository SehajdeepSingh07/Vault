package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var ErrVersionConflict = errors.New("object version conflict")
var ErrChecksumMismatch = errors.New("object checksum mismatch")

// IntegrityResult describes a full byte-level verification of a stored object version.
type IntegrityResult struct {
	Key            string `json:"key"`
	Version        uint64 `json:"version"`
	ExpectedSize   int64  `json:"expected_size"`
	ActualSize     int64  `json:"actual_size"`
	ExpectedSHA256 string `json:"expected_sha256"`
	ActualSHA256   string `json:"actual_sha256"`
	BytesVerified  int64  `json:"bytes_verified"`
	Valid          bool   `json:"valid"`
	Reason         string `json:"reason,omitempty"`
}

// VerifyVersion reads the entire content region of an object version and
// verifies both its structure and SHA-256 digest against the embedded metadata.
func (s *Store) VerifyVersion(key string, version uint64) (IntegrityResult, error) {
	if err := validateKey(key); err != nil {
		return IntegrityResult{}, err
	}
	if version == 0 {
		return IntegrityResult{}, errors.New("object version must be positive")
	}
	if err := s.checkOpen(); err != nil {
		return IntegrityResult{}, err
	}

	unlock := s.locks.lock(key)
	defer unlock()

	file, meta, offset, err := s.OpenVersion(key, version)
	if err != nil {
		return IntegrityResult{}, err
	}
	defer file.Close()

	h := sha256.New()
	n, err := io.Copy(h, io.NewSectionReader(file, offset, meta.Size))
	if err != nil {
		return IntegrityResult{}, fmt.Errorf("verify object bytes: %w", err)
	}
	actualSHA := hex.EncodeToString(h.Sum(nil))
	result := IntegrityResult{
		Key:            key,
		Version:        version,
		ExpectedSize:   meta.Size,
		ActualSize:     n,
		ExpectedSHA256: strings.ToLower(meta.SHA256),
		ActualSHA256:   actualSHA,
		BytesVerified:  n,
		Valid:          n == meta.Size && strings.EqualFold(actualSHA, meta.SHA256),
	}
	if !result.Valid {
		result.Reason = "object payload checksum or size mismatch"
	}
	return result, nil
}

// MaxObjectSize returns the configured maximum object size.
func (s *Store) MaxObjectSize() int64 { return s.maxObjectSize }

// PutVersion stores an object at an exact logical version without replacing any
// previous version. This gives the distributed replication layer copy-on-write
// semantics: a partially completed replication can be discarded safely.
//
// The returned bool is true when a new object file was created and false when an
// identical existing version was reused idempotently.
func (s *Store) PutVersion(key, contentType string, version uint64, expectedSize int64, expectedSHA string, src io.Reader) (Metadata, bool, error) {
	if err := validateKey(key); err != nil {
		return Metadata{}, false, err
	}
	if err := s.checkOpen(); err != nil {
		return Metadata{}, false, err
	}
	if version == 0 {
		return Metadata{}, false, errors.New("object version must be positive")
	}
	if expectedSize < 0 {
		return Metadata{}, false, errors.New("expected size cannot be negative")
	}
	expectedSHA = strings.ToLower(strings.TrimSpace(expectedSHA))
	if len(expectedSHA) != 64 {
		return Metadata{}, false, errors.New("expected sha256 must be 64 hexadecimal characters")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	unlock := s.locks.lock(key)
	defer unlock()

	path := s.versionObjectPath(key, uint64(version))
	if existing, err := s.readMetadataPath(path); err == nil {
		if existing.Version != uint64(version) || existing.Key != key {
			return Metadata{}, false, ErrVersionConflict
		}
		if existing.Size != expectedSize || !strings.EqualFold(existing.SHA256, expectedSHA) {
			return Metadata{}, false, ErrVersionConflict
		}
		return existing, false, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Metadata{}, false, err
	}

	tmp, err := os.CreateTemp(s.objectsDir, ".version-upload-*")
	if err != nil {
		return Metadata{}, false, fmt.Errorf("create version temp object: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	defer cleanup()

	if _, err := tmp.Write(fileMagic[:]); err != nil {
		return Metadata{}, false, fmt.Errorf("write object magic: %w", err)
	}

	h := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(src, s.maxObjectSize+1))
	if err != nil {
		return Metadata{}, false, fmt.Errorf("write object data: %w", err)
	}
	if written > s.maxObjectSize {
		return Metadata{}, false, ErrObjectTooLarge
	}
	if written != expectedSize {
		return Metadata{}, false, fmt.Errorf("object size mismatch: got %d want %d", written, expectedSize)
	}
	actualSHA := hex.EncodeToString(h.Sum(nil))
	if actualSHA != expectedSHA {
		return Metadata{}, false, ErrChecksumMismatch
	}

	now := time.Now().UTC()
	meta := Metadata{
		Key:         key,
		Version:     version,
		Size:        written,
		SHA256:      actualSHA,
		ContentType: contentType,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	metaBytes, err := marshalMetadata(meta)
	if err != nil {
		return Metadata{}, false, err
	}
	if _, err := tmp.Write(metaBytes); err != nil {
		return Metadata{}, false, fmt.Errorf("write object metadata: %w", err)
	}
	if err := writeFooter(tmp, uint64(len(metaBytes))); err != nil {
		return Metadata{}, false, err
	}
	if err := tmp.Sync(); err != nil {
		return Metadata{}, false, fmt.Errorf("sync version object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Metadata{}, false, fmt.Errorf("close version object: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return Metadata{}, false, fmt.Errorf("commit version object: %w", err)
	}
	if err := syncDirectory(s.objectsDir); err != nil {
		return Metadata{}, false, fmt.Errorf("sync version object directory: %w", err)
	}
	return meta, true, nil
}

func (s *Store) MetadataVersion(key string, version uint64) (Metadata, error) {
	if err := validateKey(key); err != nil {
		return Metadata{}, err
	}
	if version == 0 {
		return Metadata{}, errors.New("object version must be positive")
	}
	if err := s.checkOpen(); err != nil {
		return Metadata{}, err
	}
	return s.readMetadataPath(s.versionObjectPath(key, version))
}

func (s *Store) OpenVersion(key string, version uint64) (*os.File, Metadata, int64, error) {
	if err := validateKey(key); err != nil {
		return nil, Metadata{}, 0, err
	}
	if version == 0 {
		return nil, Metadata{}, 0, errors.New("object version must be positive")
	}
	if err := s.checkOpen(); err != nil {
		return nil, Metadata{}, 0, err
	}
	file, err := os.Open(s.versionObjectPath(key, version))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, Metadata{}, 0, ErrNotFound
		}
		return nil, Metadata{}, 0, fmt.Errorf("open version object: %w", err)
	}
	meta, dataOffset, err := readMetadataAndOffset(file)
	if err != nil {
		_ = file.Close()
		return nil, Metadata{}, 0, err
	}
	if meta.Key != key || meta.Version != version {
		_ = file.Close()
		return nil, Metadata{}, 0, ErrInvalidObject
	}
	return file, meta, dataOffset, nil
}

func (s *Store) DeleteVersion(key string, version uint64) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if version == 0 {
		return errors.New("object version must be positive")
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	unlock := s.locks.lock(key)
	defer unlock()
	path := s.versionObjectPath(key, version)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("delete version object: %w", err)
	}
	return syncDirectory(s.objectsDir)
}

// DeleteVersionsBefore removes versioned copies older than keepVersion. It is
// best-effort garbage collection used after a metadata commit.
func (s *Store) DeleteVersionsBefore(key string, keepVersion uint64) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if keepVersion == 0 {
		return errors.New("keep version must be positive")
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	unlock := s.locks.lock(key)
	defer unlock()
	pattern := filepath.Join(s.objectsDir, versionFilePrefix(key)+".*.vobj")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("find versioned objects: %w", err)
	}
	var firstErr error
	for _, path := range matches {
		meta, err := s.readMetadataPath(path)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if meta.Version < keepVersion {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := syncDirectory(s.objectsDir); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (s *Store) versionObjectPath(key string, version uint64) string {
	return filepath.Join(s.objectsDir, versionFilePrefix(key)+"."+strconv.FormatUint(version, 10)+".vobj")
}

func versionFilePrefix(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
