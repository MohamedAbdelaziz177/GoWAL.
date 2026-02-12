package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type WalEntry struct {
	Op    string
	Key   string
	Value string
}

type WAL struct {
	file   *os.File
	mu     sync.RWMutex
	writer *bufio.Writer
	path   string
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   path,
	}, nil
}

func (w *WAL) Append(entry WalEntry) error {

	w.mu.Lock()
	defer w.mu.Unlock()

	data := serializeEntry(entry)
	legnth := uint32(len(data))

	if err := binary.Write(w.writer, binary.LittleEndian, legnth); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	_, err := w.writer.Write(data)

	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	checksum := crc32.ChecksumIEEE(data)
	if err := binary.Write(w.writer, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

	/*could be replaced by a background goroutine that syncs periodically --*/

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

func (w *WAL) ReadEntries() ([]WalEntry, error) {

	w.mu.RLock()
	defer w.mu.RUnlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek file: %w", err)
	}

	reader := bufio.NewReader(w.file)
	//var entries []WalEntry

	for {
		var length uint32
		err := binary.Read(reader, binary.LittleEndian, &length)

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read length: %w", err)
		}
	}
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	w.writer.Reset(w.file)

	return nil
}

func serializeEntry(entry WalEntry) []byte {

	opLen := uint32(len(entry.Op))
	keyLen := uint32(len(entry.Key))
	valueLen := uint32(len(entry.Value))

	bufferSize := 12 + opLen + keyLen + valueLen
	buffer := make([]byte, bufferSize)

	offset := 0

	binary.BigEndian.PutUint32(buffer[offset:offset+4], opLen)
	offset += 4

	copy(buffer[offset:], entry.Op)
	offset += int(opLen)

	binary.BigEndian.PutUint32(buffer[offset:offset+4], keyLen)
	offset += 4

	copy(buffer[offset:], entry.Key)
	offset += int(keyLen)

	binary.BigEndian.PutUint32(buffer[offset:offset+4], valueLen)
	offset += 4

	copy(buffer[offset:], entry.Value)

	return buffer

}

func deserializeEntry(data []byte) (WalEntry, error) {

	var entry WalEntry
	offset := 0

	opLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	entry.Op = string(data[offset : offset+int(opLen)])
	offset += int(opLen)

	keyLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	entry.Key = string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	entry.Value = string(data[offset : offset+int(valueLen)])
	return entry, nil
}
