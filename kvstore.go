package main

import (
	"fmt"
	"os"
	"sync"
)

type KVStore struct {
	data map[string]string
	wal  *WAL
	mu   sync.RWMutex
}

func NewKVStore(walPath string) (*KVStore, error) {

	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	kv := &KVStore{
		data: make(map[string]string),
		wal:  wal,
	}

	if err := kv.Recover(); err != nil {

		wal.Close()
		return nil, fmt.Errorf("failed to recover: %w", err)
	}

	return kv, nil
}

func (kv *KVStore) Set(key, value string) error {

	entry := WalEntry{
		Op:    "SET",
		Key:   key,
		Value: value,
	}

	if err := kv.wal.Append(entry); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	kv.mu.Lock()
	kv.data[key] = value
	kv.mu.Unlock()

	return nil
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, exists := kv.data[key]
	return value, exists
}

func (kv *KVStore) Delete(key string) error {

	value, _ := kv.Get(key)

	entry := WalEntry{
		Op:    "DELETE",
		Key:   key,
		Value: value,
	}

	if err := kv.wal.Append(entry); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	kv.mu.Lock()
	delete(kv.data, key)
	kv.mu.Unlock()

	return nil
}

func (kv *KVStore) Recover() error {

	entries, err := kv.wal.ReadEntries()

	if err != nil {
		return err
	}

	for _, v := range entries {
		switch v.Op {
		case "SET":
			kv.data[v.Key] = v.Value
		case "DELETE":
			delete(kv.data, v.Key)
		default:
			return fmt.Errorf("unknown operation: %s", v.Op)

		}
	}

	return nil
}

func (kv *KVStore) Checkpoint(snapshotPath string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	file, err := os.Create(snapshotPath)

	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	defer file.Close()

	for k, v := range kv.data {

		line := fmt.Sprintf("%s=%s\n", k, v)

		_, err := file.WriteString(line)

		if err != nil {
			return fmt.Errorf("failed to write snapshot: %w", err)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot: %w", err)
	}

	if err := kv.wal.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	return nil
}

func (kv *KVStore) Close() error {
	return kv.wal.Close()
}

func (kv *KVStore) Size() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return len(kv.data)
}
