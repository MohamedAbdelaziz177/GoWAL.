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

	/*
			TrxId int
		    LSN int
	*/
}

type WAL struct {
	file         *os.File
	mu           sync.RWMutex
	writer       *bufio.Writer
	path         string
	batchCount   int
	syncInterval int
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:         file,
		writer:       bufio.NewWriter(file),
		path:         path,
		batchCount:   0,
		syncInterval: 10,
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

	w.batchCount++

	if w.batchCount >= 10 {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}

		w.batchCount = 0
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
	var entries []WalEntry

	for {
		var length uint32
		err := binary.Read(reader, binary.LittleEndian, &length)

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read length: %w", err)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}

		var checksum uint32
		if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
			return nil, fmt.Errorf("failed to read checksum: %w", err)
		}

		if crc32.ChecksumIEEE(data) != checksum {
			return nil, fmt.Errorf("data corruption detected")
		}

		entry, err := deserializeEntry(data)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize: %w", err)
		}

		entries = append(entries, entry)

	}

	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return entries, nil
}

func (w *WAL) fsync() error {

	// w.mu.Lock()
	// defer w.mu.Unlock()     ------- if only used when Closing the Wal, maybe no need for locking --- they will lead to deadlock

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("Failed to flush during force sync ")
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("Failed to force sync ")
	}

	return nil
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

func (w *WAL) Close() error {

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.fsync(); err != nil {
		return fmt.Errorf("failed to sync data: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}

func serializeEntry(entry WalEntry) []byte {

	opLen := uint32(len(entry.Op))
	keyLen := uint32(len(entry.Key))
	valueLen := uint32(len(entry.Value))

	bufferSize := 12 + opLen + keyLen + valueLen
	buffer := make([]byte, bufferSize)

	offset := 0

	binary.LittleEndian.PutUint32(buffer[offset:offset+4], opLen)
	offset += 4

	copy(buffer[offset:], entry.Op)
	offset += int(opLen)

	binary.LittleEndian.PutUint32(buffer[offset:offset+4], keyLen)
	offset += 4

	copy(buffer[offset:], entry.Key)
	offset += int(keyLen)

	binary.LittleEndian.PutUint32(buffer[offset:offset+4], valueLen)
	offset += 4

	copy(buffer[offset:], entry.Value)

	return buffer

}

func deserializeEntry(data []byte) (WalEntry, error) {

	var entry WalEntry
	offset := 0

	opLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	entry.Op = string(data[offset : offset+int(opLen)])
	offset += int(opLen)

	keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	entry.Key = string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	valueLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	entry.Value = string(data[offset : offset+int(valueLen)])
	return entry, nil
}
