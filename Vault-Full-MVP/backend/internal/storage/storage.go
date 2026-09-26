package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound       = errors.New("object not found")
	ErrInvalidObject  = errors.New("object file is invalid")
	ErrObjectTooLarge = errors.New("object exceeds configured maximum size")
)

var (
	fileMagic   = [8]byte{'V', 'A', 'U', 'L', 'T', 'O', 'B', 'J'}
	footerMagic = [8]byte{'V', 'A', 'U', 'L', 'T', 'E', 'N', 'D'}
)

const maxMetadataSize = 1 << 20

type Store struct {
	root          string
	objectsDir    string
	maxObjectSize int64
	locks         *keyLocker
	closed        bool
	closeMu       sync.RWMutex
}

func New(root string, maxObjectSize int64) (*Store, error) {
	if root == "" {
		return nil, errors.New("storage root cannot be empty")
	}
	if maxObjectSize <= 0 {
		return nil, errors.New("max object size must be positive")
	}

	objectsDir := filepath.Join(root, "objects")
	if err := os.MkdirAll(objectsDir, 0o750); err != nil {
		return nil, fmt.Errorf("create storage directories: %w", err)
	}

	return &Store{
		root:          root,
		objectsDir:    objectsDir,
		maxObjectSize: maxObjectSize,
		locks:         newKeyLocker(),
	}, nil
}

func (s *Store) Put(key, contentType string, src io.Reader) (Metadata, error) {
	if err := validateKey(key); err != nil {
		return Metadata{}, err
	}
	if err := s.checkOpen(); err != nil {
		return Metadata{}, err
	}

	unlock := s.locks.lock(key)
	defer unlock()

	path := s.objectPath(key)
	previous, err := s.readMetadataPath(path)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Metadata{}, err
	}

	version := uint64(1)
	createdAt := time.Now().UTC()
	if err == nil {
		version = previous.Version + 1
		createdAt = previous.CreatedAt
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	tmp, err := os.CreateTemp(s.objectsDir, ".upload-*")
	if err != nil {
		return Metadata{}, fmt.Errorf("create temp object: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	defer cleanup()

	if _, err := tmp.Write(fileMagic[:]); err != nil {
		return Metadata{}, fmt.Errorf("write object magic: %w", err)
	}

	h := sha256.New()
	limited := io.LimitReader(src, s.maxObjectSize+1)
	written, err := io.Copy(io.MultiWriter(tmp, h), limited)
	if err != nil {
		return Metadata{}, fmt.Errorf("write object data: %w", err)
	}
	if written > s.maxObjectSize {
		return Metadata{}, ErrObjectTooLarge
	}

	now := time.Now().UTC()
	meta := Metadata{
		Key:         key,
		Version:     version,
		Size:        written,
		SHA256:      hex.EncodeToString(h.Sum(nil)),
		ContentType: contentType,
		CreatedAt:   createdAt,
		UpdatedAt:   now,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return Metadata{}, fmt.Errorf("marshal metadata: %w", err)
	}
	if len(metaBytes) > maxMetadataSize {
		return Metadata{}, errors.New("metadata exceeds maximum size")
	}

	if _, err := tmp.Write(metaBytes); err != nil {
		return Metadata{}, fmt.Errorf("write metadata: %w", err)
	}
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(metaBytes)))
	if _, err := tmp.Write(lenBuf[:]); err != nil {
		return Metadata{}, fmt.Errorf("write metadata length: %w", err)
	}
	if _, err := tmp.Write(footerMagic[:]); err != nil {
		return Metadata{}, fmt.Errorf("write footer magic: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		return Metadata{}, fmt.Errorf("sync object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Metadata{}, fmt.Errorf("close temp object: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return Metadata{}, fmt.Errorf("commit object: %w", err)
	}
	if err := syncDirectory(s.objectsDir); err != nil {
		return Metadata{}, fmt.Errorf("sync object directory: %w", err)
	}

	return meta, nil
}

func (s *Store) Metadata(key string) (Metadata, error) {
	if err := validateKey(key); err != nil {
		return Metadata{}, err
	}
	if err := s.checkOpen(); err != nil {
		return Metadata{}, err
	}
	return s.readMetadataPath(s.objectPath(key))
}

func (s *Store) Open(key string) (*os.File, Metadata, int64, error) {
	if err := validateKey(key); err != nil {
		return nil, Metadata{}, 0, err
	}
	if err := s.checkOpen(); err != nil {
		return nil, Metadata{}, 0, err
	}

	path := s.objectPath(key)
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, Metadata{}, 0, ErrNotFound
		}
		return nil, Metadata{}, 0, fmt.Errorf("open object: %w", err)
	}

	meta, dataOffset, err := readMetadataAndOffset(file)
	if err != nil {
		_ = file.Close()
		return nil, Metadata{}, 0, err
	}
	if meta.Key != key {
		_ = file.Close()
		return nil, Metadata{}, 0, ErrInvalidObject
	}
	return file, meta, dataOffset, nil
}

func (s *Store) Delete(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}

	unlock := s.locks.lock(key)
	defer unlock()

	path := s.objectPath(key)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("stat object: %w", err)
	}

	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("delete object: %w", err)
	}
	if err := syncDirectory(s.objectsDir); err != nil {
		return fmt.Errorf("sync object directory: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.closed = true
	return nil
}

func (s *Store) objectPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.objectsDir, hex.EncodeToString(sum[:])+".obj")
}

func (s *Store) readMetadataPath(path string) (Metadata, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Metadata{}, ErrNotFound
		}
		return Metadata{}, fmt.Errorf("open object metadata: %w", err)
	}
	defer file.Close()

	meta, _, err := readMetadataAndOffset(file)
	return meta, err
}

func (s *Store) checkOpen() error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return errors.New("storage is closed")
	}
	return nil
}

func validateKey(key string) error {
	if key == "" {
		return errors.New("object key cannot be empty")
	}
	if len(key) > 4096 {
		return errors.New("object key exceeds 4096 bytes")
	}
	if strings.IndexByte(key, 0) >= 0 {
		return errors.New("object key contains NUL byte")
	}
	return nil
}

func readMetadataAndOffset(file *os.File) (Metadata, int64, error) {
	info, err := file.Stat()
	if err != nil {
		return Metadata{}, 0, fmt.Errorf("stat object: %w", err)
	}
	if info.Size() < int64(len(fileMagic)+16) {
		return Metadata{}, 0, ErrInvalidObject
	}

	var magic [8]byte
	if _, err := file.ReadAt(magic[:], 0); err != nil {
		return Metadata{}, 0, ErrInvalidObject
	}
	if magic != fileMagic {
		return Metadata{}, 0, ErrInvalidObject
	}

	footer := make([]byte, 16)
	if _, err := file.ReadAt(footer, info.Size()-16); err != nil {
		return Metadata{}, 0, ErrInvalidObject
	}
	if !equalBytes(footer[8:], footerMagic[:]) {
		return Metadata{}, 0, ErrInvalidObject
	}

	metaLen := binary.BigEndian.Uint64(footer[:8])
	if metaLen == 0 || metaLen > maxMetadataSize {
		return Metadata{}, 0, ErrInvalidObject
	}
	metaStart := info.Size() - 16 - int64(metaLen)
	dataOffset := int64(len(fileMagic))
	if metaStart < dataOffset {
		return Metadata{}, 0, ErrInvalidObject
	}

	metaBytes := make([]byte, metaLen)
	if _, err := file.ReadAt(metaBytes, metaStart); err != nil {
		return Metadata{}, 0, ErrInvalidObject
	}

	var meta Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return Metadata{}, 0, ErrInvalidObject
	}
	if meta.Key == "" || meta.Version == 0 || meta.Size < 0 || meta.SHA256 == "" {
		return Metadata{}, 0, ErrInvalidObject
	}
	if dataBytes := metaStart - dataOffset; dataBytes != meta.Size {
		return Metadata{}, 0, ErrInvalidObject
	}
	return meta, dataOffset, nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
