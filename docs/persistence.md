# Persistence

## Overview

NucleoDB provides two persistence mechanisms: **SaveHandler** for periodic snapshots of in-memory state to disk, and **ExportHandler** for writing a JSON log of all modification events. Both operate as background threads and are optional — controlled by configuration flags.

## SaveHandler (Snapshot Persistence)

**Source:** `database/tables/table/SaveHandler.java` and `database/tables/connection/SaveHandler.java`

SaveHandler periodically serializes the entire DataTable or ConnectionHandler to disk using Java's `ObjectOutputStream`. This provides fast startup by loading the snapshot and only replaying events from Kafka that occurred after the snapshot.

### How It Works

```
SaveHandler Thread (runs continuously)
    │
    ▼
┌───────────────────────────────────────┐
│  while (true):                        │
│    if (table.changed > lastSaved):    │
│      ObjectFileWriter.writeObject(    │
│        table,                         │
│        config.getTableFileName()      │
│      )                                │
│      lastSaved = table.changed        │
│    Thread.sleep(config.saveInterval)  │
└───────────────────────────────────────┘
```

### Save Trigger

The `changed` field on DataTable/ConnectionHandler is a timestamp that is updated every time a modification is processed. SaveHandler compares this against the last saved timestamp to determine if a write is needed.

```java
// Updated in DataTable.modify() after processing any event
this.changed = new Date().getTime();

// Updated in consumerResponse() as well
this.changed = new Date().getTime();
```

### Save Interval

The save frequency is controlled by `DataTableConfig.saveInterval` (milliseconds). The SaveHandler sleeps for this duration between checks.

### File Format

Files are written using Java's `ObjectOutputStream` via `ObjectFileWriter`:

```
┌─────────────────────────────────────┐
│  ObjectFileWriter.writeObjectToFile │
│                                     │
│  1. Create FileOutputStream         │
│  2. Create ObjectOutputStream       │
│  3. writeObject(dataTable)          │
│  4. Close streams                   │
│                                     │
│  Output: Binary Java serialization  │
│  of the entire DataTable object     │
└─────────────────────────────────────┘
```

### File Naming

| Entity | File Name Pattern | Example |
|--------|------------------|---------|
| DataTable | `{tableName}.dat` | `author.dat` |
| ConnectionHandler | `{label}.dat` | `AUTHORBOOK.dat` |

Configured via `DataTableConfig.getTableFileName()` and `ConnectionConfig.getConnectionFileName()`.

### What Is Serialized

For **DataTable**:
- `entries` — All DataEntry objects (Set)
- `keyToEntry` — UUID-to-entry map
- `partitionOffsets` — Kafka partition offsets (for resume)
- `changed` — Last modification timestamp
- `deletedEntries` — Set of deleted UUIDs
- `config` (partial) — Merged during load

For **ConnectionHandler**:
- `allConnections` — All Connection objects (Set)
- `partitionOffsets` — Kafka partition offsets
- `changed` — Last modification timestamp
- `consumerId` — Consumer group ID
- `deletedEntries` — Set of deleted UUIDs

Transient fields (indexes, producers, consumers, threads) are **not** serialized and are rebuilt on load.

## Loading Saved Data

**Source:** `DataTable.loadSavedData()` and `ConnectionHandler.loadSavedData()`

### DataTable Load

```java
public void loadSavedData() {
    if (new File(config.getTableFileName()).exists()) {
        DataTable tmpTable = (DataTable) new ObjectFileReader()
            .readObjectFromFile(config.getTableFileName());

        // Merge configuration
        if (tmpTable.config != null)
            this.config.merge(tmpTable.config);

        // Restore state
        this.changed = tmpTable.changed;
        this.entries = tmpTable.entries;
        this.partitionOffsets = tmpTable.partitionOffsets;
        this.keyToEntry = tmpTable.keyToEntry;

        // Rebuild indexes (not serialized)
        this.entries.forEach(entry -> {
            for (IndexWrapper i : this.indexes.values()) {
                i.add(entry);
            }
            entry.dataTable = this;
            entry.setTableName(this.config.getTable());
        });
    }
}
```

### ConnectionHandler Load

```java
public void loadSavedData() {
    if (new File(config.getConnectionFileName()).exists()) {
        ConnectionHandler tmpConnections = (ConnectionHandler)
            new ObjectFileReader().readObjectFromFile(config.getConnectionFileName());

        // Restore connections (rebuilds all index maps)
        tmpConnections.allConnections.forEach(c -> this.addConnection(c));

        // Restore metadata
        this.changed = tmpConnections.changed;
        this.consumerId = tmpConnections.getConsumerId();
        this.partitionOffsets = tmpConnections.partitionOffsets;
    }
}
```

### Partition Offset Resume

The key optimization is storing `partitionOffsets`. When the Kafka consumer starts, it seeks to the stored offsets instead of reading from the beginning:

```
Without SaveHandler:
  Kafka topic: [event1, event2, ..., event1000000]
  Startup: replay ALL 1,000,000 events → slow

With SaveHandler:
  Snapshot: entries as of event 999,000
  Kafka topic: [event1, event2, ..., event1000000]
  Startup: load snapshot, seek to offset 999,000
  Replay only 1,000 new events → fast
```

## ExportHandler (JSON Modification Log)

**Source:** `database/tables/table/ExportHandler.java` and `database/tables/connection/ExportHandler.java`

ExportHandler writes a JSON-formatted log of all incoming modification events. This is useful for auditing, debugging, external replication, or migration.

### How It Works

```
ExportHandler Thread (runs continuously)
    │
    ▼
┌────────────────────────────────────────────┐
│  modifications: Queue<String>              │
│  (populated by QueueHandler when           │
│   config.isJsonExport() is true)           │
│                                            │
│  while (true):                             │
│    poll modifications from queue           │
│    write each as a line to JSON file       │
│    flush periodically                      │
└────────────────────────────────────────────┘
```

### Event Capture

When `jsonExport` is enabled, QueueHandler adds raw message strings to the ExportHandler's queue:

```java
// In QueueHandler.dataTableType()
if (database.getConfig().isJsonExport()) {
    database.getExportHandler().getModifications().add(entry);
}

// In QueueHandler.connectionType()
if (connectionHandler.getConfig().isJsonExport()) {
    connectionHandler.getExportHandler().getModifications().add(entry);
}
```

The raw string includes the type prefix and full JSON payload, preserving the exact message format.

## Configuration Flags

### DataTable Persistence

| Flag | Default | Description |
|------|---------|-------------|
| `loadSave` | `true` | Load saved snapshot on startup |
| `saveChanges` | `true` | Enable periodic disk snapshots |
| `jsonExport` | `false` | Enable JSON modification logging |
| `saveInterval` | (configurable) | Milliseconds between save checks |

### ConnectionHandler Persistence

| Flag | Default | Description |
|------|---------|-------------|
| `loadSaved` | `true` | Load saved snapshot on startup |
| `saveChanges` | `true` | Enable periodic disk snapshots |
| `jsonExport` | `false` | Enable JSON modification logging |

### DBType Impact on Persistence

| DBType | loadSave | saveChanges | jsonExport |
|--------|----------|-------------|------------|
| `ALL` | true | true | false |
| `NO_LOCAL` | **false** | **false** | false |
| `READ_ONLY` | true | true | false |
| `EXPORT` | true | true | **true** |

## Data Flow with Persistence

```
                    ┌──────────────┐
                    │  Kafka Topic │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Consumer     │
                    │ Handler      │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
       ┌────────────┐┌──────────┐┌───────────┐
       │ QueueHandler││QueueHdlr ││ QueueHdlr │
       │ (JSON exp.) ││(modify)  ││  (modify) │
       └──────┬─────┘└────┬─────┘└─────┬─────┘
              │            │            │
              ▼            ▼            ▼
       ┌────────────┐┌──────────────────────┐
       │ Export      ││  DataTable.modify()  │
       │ Handler     ││                      │
       │             ││  entries updated     │
       │ writes JSON ││  changed = now()     │
       │ to file     ││                      │
       └─────────────┘└──────────┬───────────┘
                                 │
                                 │ (periodically)
                                 ▼
                          ┌────────────┐
                          │ SaveHandler│
                          │            │
                          │ if changed:│
                          │  serialize │
                          │  to .dat   │
                          └────────────┘
```

## File I/O Classes

### ObjectFileWriter

```java
// Writes a Serializable object to a file
public void writeObjectToFile(Object obj, String fileName)
```

Uses `FileOutputStream` → `ObjectOutputStream` → `writeObject()`.

### ObjectFileReader

```java
// Reads a Serializable object from a file
public Object readObjectFromFile(String fileName)
```

Uses `FileInputStream` → `ObjectInputStream` → `readObject()`.

## Considerations

1. **Snapshot consistency**: SaveHandler writes the entire object atomically. If the process crashes mid-write, the file may be corrupted. The next startup will fail to load and will replay from Kafka entirely.

2. **Disk space**: Snapshots contain the full in-memory state. For large datasets, this can be significant.

3. **GC pressure**: After loading a saved snapshot and rebuilding indexes, a `System.gc()` call is triggered to reclaim temporary objects.

4. **Export size**: The JSON export grows unboundedly. External log rotation should be configured.

5. **Thread safety**: SaveHandler serializes the DataTable while modifications may be occurring. The `entries` set is synchronized during modifications but not during serialization — the snapshot may contain a slightly inconsistent view that is corrected by subsequent Kafka replay.
