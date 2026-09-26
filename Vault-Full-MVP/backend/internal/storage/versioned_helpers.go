package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
)

func marshalMetadata(meta Metadata) ([]byte, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	if len(data) > maxMetadataSize {
		return nil, fmt.Errorf("metadata exceeds maximum size")
	}
	return data, nil
}

func writeFooter(f *os.File, metaLen uint64) error {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], metaLen)
	if _, err := f.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write metadata length: %w", err)
	}
	if _, err := f.Write(footerMagic[:]); err != nil {
		return fmt.Errorf("write footer magic: %w", err)
	}
	return nil
}
