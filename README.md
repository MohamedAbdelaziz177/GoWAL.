# Write-Ahead Log (WAL) Implementation in Go

A Write-Ahead Log implementation with crash recovery and checkpointing, demonstrated with a simple Key-Value store.

## Features

- Crash recovery with WAL replay
- CRC32 checksums for data integrity
- Batch fsync (every 10 operations)
- Thread-safe operations
- Checkpointing with WAL truncation

## How It Works

### Write Flow
```
Client → Append to WAL → Update In-Memory Map → Fsync (every 10 ops)
```

### Recovery Flow
```
Startup → Read WAL Entries → Replay Operations → Ready
```

## Installation

```bash
go get
go build
```

## Usage

```go
// Create KV store with WAL
kv, err := NewKVStore("data/demo.wal")
if err != nil {
    log.Fatal(err)
}
defer kv.Close()

// Write operations
kv.Set("key1", "value1")
kv.Set("key2", "value2")

// Read
value, exists := kv.Get("key1")

// Delete
kv.Delete("key2")

// Checkpoint (snapshot + truncate WAL)
kv.Checkpoint("data/snapshot.db")
```

## WAL Operations

### Append
Writes entry to WAL file with format: `[Length(4)] [Data(N)] [CRC32(4)]`

```
Entry → Serialize → Write Length → Write Data → Write Checksum → Flush → Fsync (every 10)
```

### ReadEntries
Reads all entries for recovery:
```
Seek Start → Read Length → Read Data → Verify Checksum → Deserialize → Repeat
```

### Truncate
Clears the WAL file (called after checkpoint):
```
Truncate File → Seek to Start → Reset Buffer
```

## Data Format

Each entry is serialized as:
```
[OpLen(4)] [Op] [KeyLen(4)] [Key] [ValueLen(4)] [Value]
```

WAL file structure:
```
[Length(4)] [SerializedEntry] [CRC32(4)]
[Length(4)] [SerializedEntry] [CRC32(4)]
...
```

## Crash Recovery

On startup, the KV store automatically:
1. Reads all WAL entries
2. Replays each operation (SET/DELETE)
3. Rebuilds the in-memory state

Example:
```go
// Before crash:
kv.Set("key1", "value1")
kv.Set("key2", "value2")
kv.Delete("key1")
// CRASH!

// After restart:
newKv, _ := NewKVStore("data/demo.wal")
// Automatically recovers: {"key2": "value2"}
```

## Checkpointing

Creates a snapshot of current state and clears the WAL:

```go
kv.Checkpoint("snapshot.db")
// Writes: key1=value1\nkey2=value2\n...
// Then truncates WAL to 0 bytes
```

## API

### KVStore
- `NewKVStore(walPath string)` - Create store with recovery
- `Set(key, value string)` - Set key-value pair
- `Get(key string) (string, bool)` - Get value by key
- `Delete(key string)` - Delete key
- `Checkpoint(snapshotPath string)` - Snapshot and truncate WAL
- `Close()` - Close store

### WAL
- `NewWAL(path string)` - Create WAL
- `Append(entry WalEntry)` - Write entry
- `ReadEntries()` - Read all entries
- `Truncate()` - Clear WAL file
- `Close()` - Close WAL

## Example

See `main.go` for a complete example demonstrating:
- Writing multiple key-value pairs
- Creating a checkpoint
- Simulating a crash (closing the store)
- Recovering from WAL after restart
