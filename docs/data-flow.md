# Data Flow

## Overview

NucleoDB's data flow is built on an event-sourcing pattern where all writes are first published to a message queue (Kafka) and then consumed back — even by the originating node. This ensures all nodes converge to the same state. Reads are served directly from in-memory data structures.

## Write Flow (Create)

### Synchronous Create: `table.saveSync(entry)`

```
User Code                   DataTable                   ProducerHandler            Kafka              ConsumerHandler           QueueHandler              DataTable.modify()
    │                           │                            │                      │                      │                       │                          │
    │ saveSync(entry)           │                            │                      │                      │                       │                          │
    ├──────────────────────────►│                            │                      │                      │                       │                          │
    │                           │ saveInternalConsumer()     │                      │                      │                       │                          │
    │                           │ ┌──────────────────────┐   │                      │                      │                       │                          │
    │                           │ │ 1. Generate changeUUID│  │                      │                      │                       │                          │
    │                           │ │ 2. Create CountDown   │  │                      │                      │                       │                          │
    │                           │ │    Latch(1)           │  │                      │                      │                       │                          │
    │                           │ │ 3. Register callback  │  │                      │                      │                       │                          │
    │                           │ │    in changeListeners │  │                      │                      │                       │                          │
    │                           │ └──────────────────────┘   │                      │                      │                       │                          │
    │                           │                            │                      │                      │                       │                          │
    │                           │ saveInternal()             │                      │                      │                       │                          │
    │                           │ ┌──────────────────────┐   │                      │                      │                       │                          │
    │                           │ │ Entry not in set?     │  │                      │                      │                       │                          │
    │                           │ │ → Create new Create   │  │                      │                      │                       │                          │
    │                           │ │   modification object │  │                      │                      │                       │                          │
    │                           │ └──────────┬───────────┘   │                      │                      │                       │                          │
    │                           │            │               │                      │                      │                       │                          │
    │                           │            │ push(key,     │                      │                      │                       │                          │
    │                           │            │  version,     │                      │                      │                       │                          │
    │                           │            │  Create, cb)  │                      │                      │                       │                          │
    │                           │            └──────────────►│                      │                      │                       │                          │
    │                           │                            │ produce("CREATE"+    │                      │                       │                          │
    │                           │                            │  json)               │                      │                       │                          │
    │                           │                            ├─────────────────────►│                      │                       │                          │
    │                           │                            │                      │                      │                       │                          │
    │  ┌─────── latch.await() ──┤                            │                      │  poll()              │                       │                          │
    │  │  (thread blocks)       │                            │                      ├─────────────────────►│                       │                          │
    │  │                        │                            │                      │                      │ add to queue           │                          │
    │  │                        │                            │                      │                      ├──────────────────────►│                          │
    │  │                        │                            │                      │                      │                       │ dataTableType()          │
    │  │                        │                            │                      │                      │                       │ parse "CREATE" prefix    │
    │  │                        │                            │                      │                      │                       │ deserialize Create obj   │
    │  │                        │                            │                      │                      │                       │                          │
    │  │                        │                            │                      │                      │                       │ modify(CREATE, create)   │
    │  │                        │                            │                      │                      │                       ├─────────────────────────►│
    │  │                        │                            │                      │                      │                       │                          │
    │  │                        │                            │                      │                      │                       │    ┌──────────────────┐  │
    │  │                        │                            │                      │                      │                       │    │ 1. Check deleted  │  │
    │  │                        │                            │                      │                      │                       │    │ 2. Check readTo   │  │
    │  │                        │                            │                      │                      │                       │    │    Time           │  │
    │  │                        │                            │                      │                      │                       │    │ 3. Check exists   │  │
    │  │                        │                            │                      │                      │                       │    │ 4. Construct new  │  │
    │  │                        │                            │                      │                      │                       │    │    DataEntry<T>   │  │
    │  │                        │                            │                      │                      │                       │    │ 5. Add to entries │  │
    │  │                        │                            │                      │                      │                       │    │ 6. Add to indexes │  │
    │  │                        │                            │                      │                      │                       │    │ 7. Add to keyMap  │  │
    │  │                        │                            │                      │                      │                       │    │ 8. Release lock   │  │
    │  │                        │                            │                      │                      │                       │    │ 9. Fire callback  │  │
    │  │                        │                            │                      │                      │                       │    │10. Fire listeners │  │
    │  │                        │                            │                      │                      │                       │    │11. Trigger events │  │
    │  │                        │                            │                      │                      │                       │    └──────────────────┘  │
    │  │                        │                            │                      │                      │                       │                          │
    │  │  callback fires ◄──────┤────────────────────────────┤──────────────────────┤──────────────────────┤───────────────────────┤──────────────────────────┤
    │  │  latch.countDown()     │                            │                      │                      │                       │                          │
    │  └────────────────────────┤                            │                      │                      │                       │                          │
    │                           │                            │                      │                      │                       │                          │
    │◄──────────────────────────┤                            │                      │                      │                       │                          │
    │  (returns true)           │                            │                      │                      │                       │                          │
```

### Key Steps in Detail

1. **`saveSync(entry)`** creates a `CountDownLatch(1)` and calls `saveInternalConsumer()`
2. **`saveInternalConsumer()`** generates a `changeUUID`, registers a callback in the `changeListeners` Guava cache, and calls `saveInternal()`
3. **`saveInternal()`** checks if the entry exists in the `entries` set:
   - **Not exists** → Creates a `Create` modification with the serialized data
   - **Exists** → Computes a JSON Patch diff and creates an `Update` modification
4. **`producer.push()`** publishes the modification to Kafka with the entry key and type prefix
5. The calling thread blocks on `latch.await()`
6. **Kafka delivers** the message to the `ConsumerHandler` (on this and all other nodes)
7. **`QueueHandler.dataTableType()`** parses the 6-character type prefix, deserializes the modification
8. **`DataTable.modify(CREATE, create)`** processes the event:
   - Validates the entry doesn't already exist
   - Constructs a new `DataEntry<T>` from the Create object
   - Adds to `entries`, `keyToEntry`, and all `indexes`
   - Calls `consumerResponse()` which fires the callback registered with `changeUUID`
9. The callback counts down the latch, unblocking the original caller

## Write Flow (Update)

### Update via JSON Patch

```
User Code                DataTable.saveInternal()             Kafka                 DataTable.modify(UPDATE)
    │                           │                               │                          │
    │  1. Modify entry.data     │                               │                          │
    │  2. saveSync(entry)       │                               │                          │
    ├──────────────────────────►│                               │                          │
    │                           │                               │                          │
    │                    ┌──────┤                               │                          │
    │                    │ Entry exists in entries set          │                          │
    │                    │ 1. versionIncrease() → v+1          │                          │
    │                    │ 2. Get current entry from keyToEntry│                          │
    │                    │ 3. JsonDiff.asJsonPatch(             │                          │
    │                    │      oldData, newData)               │                          │
    │                    │ 4. Serialize patch to JSON string    │                          │
    │                    │ 5. Create Update(changeUUID,         │                          │
    │                    │      entry, patchJson)               │                          │
    │                    └──────┤                               │                          │
    │                           │ push("UPDATE" + json) ──────►│                          │
    │                           │                               │ ────────────────────────►│
    │                           │                               │                          │
    │                           │                               │    ┌──────────────────┐  │
    │                           │                               │    │ 1. Lookup by key  │  │
    │                           │                               │    │ 2. Version check: │  │
    │                           │                               │    │    de.ver+1==u.ver│  │
    │                           │                               │    │ 3. Apply patch:   │  │
    │                           │                               │    │    patch.apply(    │  │
    │                           │                               │    │      oldData)     │  │
    │                           │                               │    │ 4. Set version    │  │
    │                           │                               │    │ 5. Set modified   │  │
    │                           │                               │    │ 6. Update indexes │  │
    │                           │                               │    │    per operation  │  │
    │                           │                               │    │ 7. Fire callback  │  │
    │                           │                               │    └──────────────────┘  │
    │                           │                               │                          │
    │◄──────────────────────────┤───────────────────────────────┤──────────────────────────┤
```

### JSON Patch Details

Updates use RFC 6902 JSON Patch for efficient delta encoding. Only the changed fields are transmitted:

```json
// Example: changing author name from "John" to "Jane"
[
  {"op": "replace", "path": "/name", "value": "Jane"}
]
```

During modification processing, each operation in the patch is examined:
- **replace/add/copy** → Updates the corresponding index if the path matches an indexed field
- **remove** → Removes the entry from the corresponding index
- **move** → No index action

## Write Flow (Delete)

```
User Code            DataTable                    Kafka              DataTable.modify(DELETE)
    │                    │                          │                        │
    │ deleteSync(entry)  │                          │                        │
    ├───────────────────►│                          │                        │
    │                    │ versionIncrease()        │                        │
    │                    │ Create Delete(           │                        │
    │                    │   changeUUID, entry)     │                        │
    │                    │ push(key, ver, delete)───►│                       │
    │                    │                          │ ──────────────────────►│
    │                    │                          │  ┌──────────────────┐  │
    │                    │                          │  │ 1. Lookup by key │  │
    │                    │                          │  │ 2. Version check │  │
    │                    │                          │  │    de.ver+1==ver │  │
    │                    │                          │  │ 3. Add to deleted│  │
    │                    │                          │  │    Entries set   │  │
    │                    │                          │  │ 4. Remove from   │  │
    │                    │                          │  │    entries/keys  │  │
    │                    │                          │  │ 5. Remove from   │  │
    │                    │                          │  │    all indexes   │  │
    │                    │                          │  │ 6. Decrement size│  │
    │                    │                          │  │ 7. Fire callback │  │
    │                    │                          │  └──────────────────┘  │
    │◄───────────────────┤──────────────────────────┤────────────────────────┤
```

Key detail: Deleted entry keys are tracked in the `deletedEntries` set to prevent re-creation from duplicate or delayed Create events.

## Read Flow

Reads are served entirely from in-memory data structures. No Kafka interaction is required.

```
User Code              DataTable                    IndexWrapper
    │                      │                            │
    │ get("name", "test")  │                            │
    ├─────────────────────►│                            │
    │                      │                            │
    │                      │ (key == "id"?)             │
    │                      │ No → lookup IndexWrapper   │
    │                      │      for "name"            │
    │                      │                            │
    │                      │ indexWrapper.get("test")   │
    │                      ├───────────────────────────►│
    │                      │                            │
    │                      │    ┌──────────────────┐    │
    │                      │    │ TrieIndex:        │   │
    │                      │    │   traverse trie   │   │
    │                      │    │   collect entries  │   │
    │                      │    │                    │   │
    │                      │    │ TreeIndex:         │   │
    │                      │    │   TreeMap.get(val) │   │
    │                      │    └──────────────────┘    │
    │                      │                            │
    │                      │◄───────────────────────────┤
    │                      │ Set<T> results             │
    │                      │                            │
    │                      │ Apply DataEntryProjection  │
    │                      │ (copy if writable,         │
    │                      │  lock if lockUntilWrite)   │
    │                      │                            │
    │◄─────────────────────┤                            │
    │  Set<T>              │                            │
```

### Read Operations

| Method | Description | Index Type |
|--------|-------------|------------|
| `get(key, value)` | Exact match lookup | TreeIndex or TrieIndex |
| `get("id", uuid)` | Primary key lookup via `keyToEntry` map | Direct HashMap |
| `getNotEqual(key, value)` | Set difference: all entries minus matched | Any |
| `search(key, value)` | Contains/partial match | TrieIndex |
| `searchOne(key, value)` | First result from search | TrieIndex |
| `startsWith(key, str)` | Prefix match | TrieIndex |
| `endsWith(key, str)` | Suffix match | TrieIndex |
| `greaterThan(key, obj)` | Range: `>` | TreeIndex |
| `greaterThanEqual(key, obj)` | Range: `>=` | TreeIndex |
| `lessThan(key, obj)` | Range: `<` | TreeIndex |
| `lessThanEqual(key, obj)` | Range: `<=` | TreeIndex |
| `in(key, values)` | Match any in list | Any |

## Connection Data Flow

Connections follow the same event-sourcing pattern as DataTable entries. The key difference is the indexing structure.

### Connection Write Flow

```
User Code            ConnectionHandler              Kafka           ConnectionHandler.modify()
    │                      │                          │                     │
    │ saveSync(conn)       │                          │                     │
    ├─────────────────────►│                          │                     │
    │                      │                          │                     │
    │              ┌───────┤                          │                     │
    │              │ Validate: fromKey & toKey not null                     │
    │              │ Not in allConnections?                                 │
    │              │ → ConnectionCreate(uuid, conn)                        │
    │              │ Exists?                                                │
    │              │ → JsonDiff → ConnectionUpdate                         │
    │              └───────┤                          │                     │
    │                      │ push(uuid, ver, mod)────►│                    │
    │                      │                          │───────────────────►│
    │                      │                          │                     │
    │                      │                          │  ┌───────────────┐  │
    │                      │                          │  │ Deserialize   │  │
    │                      │                          │  │ Add to:       │  │
    │                      │                          │  │  connections  │  │
    │                      │                          │  │  [fromKey]    │  │
    │                      │                          │  │  [fromKey+    │  │
    │                      │                          │  │   toKey]      │  │
    │                      │                          │  │  connReverse  │  │
    │                      │                          │  │  [toKey]      │  │
    │                      │                          │  │  [toKey+      │  │
    │                      │                          │  │   fromKey]    │  │
    │                      │                          │  │  connByUUID   │  │
    │                      │                          │  │  allConns     │  │
    │                      │                          │  └───────────────┘  │
    │◄─────────────────────┤──────────────────────────┤─────────────────────┤
```

### Connection Read Flow

Connections are indexed in multiple maps for efficient lookup:

```
connections (forward index):
  fromKey          → Set<Connection>
  fromKey + toKey  → Set<Connection>

connectionsReverse (reverse index):
  toKey            → Set<Connection>
  toKey + fromKey  → Set<Connection>

connectionByUUID:
  uuid             → Connection
```

| Method | Lookup Key | Description |
|--------|-----------|-------------|
| `getByFrom(dataEntry)` | `fromKey` | All connections from an entry |
| `getByFromAndTo(from, to)` | `fromKey + toKey` | Connections between specific entries |
| `getReverseByTo(dataEntry)` | `toKey` | All connections to an entry |
| `getReverseByFromAndTo(from, to)` | `toKey + fromKey` | Reverse lookup between specific entries |
| `get()` | `allConnections` | All connections in the handler |

## Async vs Sync API

NucleoDB provides three patterns for write operations:

### Synchronous (blocking)
```java
table.saveSync(entry);      // Blocks until Kafka round-trip completes
table.deleteSync(entry);    // Returns after confirmation
```
Uses `CountDownLatch.await()` internally.

### Asynchronous (callback)
```java
table.saveAsync(entry, (savedEntry) -> {
    // Called when the modification is consumed back
});
table.deleteAsync(entry, (deletedEntry) -> {
    // Called after deletion is confirmed
});
```
Callback stored in Guava cache with 5-second TTL.

### Fire-and-forget
```java
table.saveAndForget(entry);   // Returns immediately, no confirmation
table.delete(entry);          // Returns immediately
```
No callback registered.

## Callback Mechanism

The `changeListeners` Guava cache manages the async callback lifecycle:

```
┌─────────────────────┐
│   changeListeners   │
│   (Guava Cache)     │
│                     │
│  Key: changeUUID    │  ──── stored at write time
│  Val: Consumer<T>   │  ──── callback function
│                     │
│  TTL: 5 seconds     │  ──── auto-expiration
│  Max: 10,000        │  ──── capacity limit
│  SoftValues: true   │  ──── GC-friendly
└─────────────────────┘
```

When a modification is consumed, `consumerResponse()` looks up the `changeUUID` in the cache:
- **Found** → Callback fires in a new thread, cache entry invalidated
- **Not found** → No callback (originated on another node, or fire-and-forget)
- **Expired** → `System.exit(1)` is triggered (indicates system overload — a safety mechanism)

## Complete Data Flow Summary

```
                     ┌──────────┐
                     │User Code │
                     └────┬─────┘
                          │
              ┌───────────┼───────────┐
              │           │           │
         saveSync    saveAsync   saveAndForget
              │           │           │
              └───────────┼───────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ saveInternal()  │
                 │                 │
                 │ New? → Create   │
                 │ Exists? → Diff  │
                 │    → Update     │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ ProducerHandler │
                 │  .push()        │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │  Apache Kafka   │
                 │  (or LocalMQS)  │
                 └────────┬────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
         This Node              Other Nodes
              │                       │
              ▼                       ▼
     ┌─────────────────┐    ┌─────────────────┐
     │ ConsumerHandler │    │ ConsumerHandler │
     │ → QueueHandler  │    │ → QueueHandler  │
     └────────┬────────┘    └────────┬────────┘
              │                       │
              ▼                       ▼
     ┌─────────────────┐    ┌─────────────────┐
     │ DataTable       │    │ DataTable       │
     │  .modify()      │    │  .modify()      │
     │                 │    │                 │
     │ • Version check │    │ • Version check │
     │ • Apply change  │    │ • Apply change  │
     │ • Update index  │    │ • Update index  │
     │ • Fire callback │    │ • (no callback) │
     │ • Fire events   │    │ • Fire events   │
     └─────────────────┘    └─────────────────┘
```
