# Event Sourcing

## Overview

NucleoDB is fundamentally an event-sourced database. Every state change (create, update, delete) is captured as an immutable modification event, published to a message queue, and replayed by all nodes. The current state of the database is derived by replaying these events in order from the beginning of the Kafka topic.

## Modification Types

**Source:** `database/modifications/Modification.java`

```java
public enum Modification {
    DELETE(Delete.class),
    UPDATE(Update.class),
    CREATE(Create.class),
    CONNECTIONCREATE(ConnectionCreate.class),
    CONNECTIONDELETE(ConnectionDelete.class),
    CONNECTIONUPDATE(ConnectionUpdate.class);
}
```

All modification classes extend the `Modify` base class. The enum maps type names to their deserialization classes.

### DataTable Modifications

#### Create
**Source:** `database/modifications/Create.java`

Represents a new data entry being added.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | UUID of the new entry |
| `changeUUID` | `String` | Correlation ID for callback matching |
| `data` | `String` | JSON-serialized data object |
| `masterClass` | `String` | Fully-qualified class name of the data type |
| `version` | `long` | Always 0 for new entries |
| `time` | `Instant` | Creation timestamp |

#### Update
**Source:** `database/modifications/Update.java`

Represents a modification to an existing entry using RFC 6902 JSON Patch.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | UUID of the entry being updated |
| `changeUUID` | `String` | Correlation ID for callback matching |
| `changes` | `String` | JSON Patch string (RFC 6902) |
| `version` | `long` | Expected version after update |
| `time` | `Instant` | Update timestamp |
| `request` | `String` | Lock reference ID |

The `changes` field contains a JSON Patch document. Example:
```json
[
  {"op": "replace", "path": "/name", "value": "Jane"},
  {"op": "add", "path": "/email", "value": "jane@example.com"},
  {"op": "remove", "path": "/oldField"}
]
```

Key methods:
- `getChangesPatch()` — Deserializes `changes` into a `JsonPatch` object for application
- `getOperations()` — Deserializes `changes` into a `List<JsonOperations>` for index analysis

#### Delete
**Source:** `database/modifications/Delete.java`

Represents the removal of a data entry.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | UUID of the entry to delete |
| `changeUUID` | `String` | Correlation ID |
| `version` | `long` | Expected version (current + 1) |
| `time` | `Instant` | Deletion timestamp |
| `request` | `String` | Lock reference ID |

### Connection Modifications

Connection modifications follow the same pattern but use different fields:

#### ConnectionCreate
| Field | Type | Description |
|-------|------|-------------|
| `uuid` | `String` | UUID of the connection |
| `changeUUID` | `String` | Correlation ID |
| `connectionData` | `String` | Full JSON-serialized Connection object |
| `date` | `Instant` | Creation timestamp |
| `version` | `long` | Always 0 for new connections |

#### ConnectionUpdate
| Field | Type | Description |
|-------|------|-------------|
| `uuid` | `String` | UUID of the connection |
| `changeUUID` | `String` | Correlation ID |
| `changes` | `String` | JSON Patch string |
| `version` | `long` | Expected version after update |
| `time` | `Instant` | Update timestamp |
| `request` | `String` | Lock reference ID |

#### ConnectionDelete
| Field | Type | Description |
|-------|------|-------------|
| `uuid` | `String` | UUID of the connection |
| `changeUUID` | `String` | Correlation ID |
| `version` | `long` | Expected version (current + 1) |
| `time` | `Instant` | Deletion timestamp |
| `request` | `String` | Lock reference ID |

## Version Sequencing

Every DataEntry and Connection has a monotonically increasing `version` counter. This is the cornerstone of event ordering and consistency.

### Rules

1. **Create** events have `version = 0`
2. **Update/Delete** events have `version = current_version + 1`
3. During consumption, a modification is only applied if:
   - `existing.version + 1 == modification.version`
4. If a version is already applied (`existing.version >= modification.version`), the event is silently ignored (idempotent)
5. If the version gap is too large (out of order), the event is **requeued**

### Version Check Flow

```
Incoming modification (version V)
        │
        ▼
┌─ Entry exists? ─────────────────────────────┐
│                                              │
│  No → Requeue to modqueue                   │
│       (unless already deleted)               │
│                                              │
│  Yes ─┬─ entry.version >= V?                │
│       │  → Ignore (already applied)          │
│       │                                      │
│       ├─ entry.version + 1 == V?            │
│       │  → Apply modification                │
│       │                                      │
│       └─ entry.version + 1 != V?            │
│          → Requeue (out of order)            │
│                                              │
└──────────────────────────────────────────────┘
```

### Version Increment

```java
// DataEntry.versionIncrease()
public void versionIncrease() {
    version += 1;
    this.modified = Instant.now();
}
```

This is called by the **producer side** before creating an Update or Delete modification. The version in the modification represents the **target version** (what the version should be after applying the change).

## JSON Patch (RFC 6902)

NucleoDB uses the `json-patch` library (com.github.java-json-tools) to compute and apply diffs between object states.

### Diff Computation (Producer Side)

```java
// In DataTable.saveInternal()
JsonPatch patch = JsonDiff.asJsonPatch(
    fromObject(existingEntry.getData()),   // old state
    fromObject(modifiedEntry.getData())    // new state
);
String json = Serializer.getObjectMapper().getOmNonType().writeValueAsString(patch);
```

The `fromObject()` helper serializes the data object to a `JsonNode` without type information to ensure clean diffs.

### Patch Application (Consumer Side)

```java
// In DataTable.modify() for UPDATE
de.setData(
    fromJsonNode(
        u.getChangesPatch().apply(fromObject(de.getData())),
        de.getData().getClass()
    )
);
```

1. Serialize current data to `JsonNode`
2. Apply the `JsonPatch` to produce a new `JsonNode`
3. Deserialize the result back to the data object type

### Index Updates on Patch

After applying the patch, each operation is examined for index impacts:

```java
u.getOperations().forEach(op -> {
    switch (op.getOp()) {
        case "replace":
        case "add":
        case "copy":
            // If path matches an indexed field → indexWrapper.modify(entry)
            break;
        case "remove":
            // If path matches an indexed field → indexWrapper.delete(entry)
            break;
        case "move":
            // No index action
            break;
    }
});
```

## Modification Queue (Reordering)

**Source:** `database/tables/table/ModQueueHandler.java`

When modifications arrive out of order (version gap), they are placed in the `modqueue` for later reprocessing.

### Why Out-of-Order?

Kafka guarantees order within a partition, but:
- Different keys may map to different partitions
- Network latency between producer and consumer varies
- During startup, events from saved state and new events can interleave

### Requeue Flow

```
DataTable.modify()
    │
    │ Version mismatch (de.version + 1 != mod.version)
    │
    ▼
┌─────────────────────────────────┐
│ modqueue.add(ModificationQueue  │
│   Item(mod, modification))      │
│ leftInModQueue.incrementAndGet()│
│ modqueue.notifyAll()            │
└─────────────────────────────────┘
    │
    │ (later, in ModQueueHandler thread)
    │
    ▼
┌─────────────────────────────────┐
│ Poll from modqueue              │
│ Call dataTable.modify() again   │
│ leftInModQueue.decrementAndGet()│
│                                 │
│ If leftTmp == left (no progress)│
│   → Sleep 5ms (prevent spin)   │
│                                 │
│ If queue empty & left == 0      │
│   → Wait on modqueue monitor   │
└─────────────────────────────────┘
```

The `overkillCheck` mechanism detects when the reprocessor makes no progress (left count doesn't change), indicating that the required predecessor events haven't arrived yet. In this case, it sleeps briefly to avoid CPU spinning.

## Time-Based Read Filtering

NucleoDB supports reading the database state at a specific point in time via `readToTime`:

```java
new NucleoDB(DBType.ALL, "2024-01-01T00:00:00Z", "com.mypackage");
```

When `readToTime` is set:
- All modifications with a timestamp **after** `readToTime` are silently discarded during consumption
- The resulting in-memory state represents the database as it was at that instant
- This is useful for point-in-time recovery, debugging, and historical analysis

### Filter Application

```java
// In DataTable.modify() for CREATE
if (this.config.getReadToTime() != null && c.getTime().isAfter(this.config.getReadToTime())) {
    consumerResponse(null, c.getChangeUUID());
    return; // Skip this event
}
```

## Shard Filter

The `DataTableConfig` includes a `ShardFilter` that controls which items this node owns. It replaces the
former `NodeFilter` and integrates with the `ShardAssignment` interface for hash-ring-based sharding:

```java
// In DataTable.modify() for CREATE
if (!config.getShardFilter().create(c)) {
    consumerResponse(null, c.getChangeUUID());
    return; // Filtered out — item belongs to another shard
}

// In DataTable.modify() for DELETE
if (de != null && !config.getShardFilter().delete(d, de)) {
    consumerResponse(null, d.getChangeUUID());
    return; // Filtered out — item belongs to another shard
}
```

Configure sharding via `ShardConfig` and `HashShardAssignment` (consistent hash ring):

```java
NucleoDB db = NucleoDBBuilder.create()
    .dbType(NucleoDB.DBType.ALL)
    .packages("com.example.models")
    .shardConfig(new ShardConfig(0, 3)) // shard 0 of 3
    .build();
```

## Event Listeners

Two listener mechanisms are available:

### Modification Listeners (DataTable)

```java
table.addListener(Modification.CREATE, (entry) -> {
    // Fires on every create, including from other nodes
});
table.addListener(Modification.UPDATE, (entry) -> { ... });
table.addListener(Modification.DELETE, (entry) -> { ... });
```

These are set-based (multiple listeners per modification type) and fire synchronously within the modify() method.

### Event Listeners (DataTableEventListener / ConnectionEventListener)

```java
// Configured via DataTableConfig
config.setEventListener(new DataTableEventListener() {
    public void create(Create modification, DataEntry entry) { ... }
    public void update(Update modification, DataEntry entry) { ... }
    public void delete(Delete modification, DataEntry entry) { ... }
});
```

These fire in new threads and receive both the raw modification object and the resulting entry.

## Deleted Entry Tracking

To prevent re-creation of deleted entries (due to late-arriving Create events), the `deletedEntries` set tracks all UUIDs that have been deleted:

```java
private Set<String> deletedEntries = new TreeSetExt<>();
```

During Create processing:
```java
if (deletedEntries.contains(c.getKey())) {
    return; // Entry was deleted, do not recreate
}
```

This set grows over time but prevents phantom entries from appearing.
