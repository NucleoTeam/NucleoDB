# Data Model

## Overview

NucleoDB uses an annotation-driven data model with two primary entity types: **DataEntry** (nodes/records) and **Connection** (edges/relationships). Together, they form a graph-like data structure where DataEntries are the vertices and Connections are the directed edges.

## DataEntry\<T\>

**Source:** `database/tables/table/DataEntry.java`

`DataEntry<T>` is the generic wrapper that stores a user-defined data object along with system metadata. Every record in NucleoDB is a DataEntry.

### Structure

```
┌─────────────────────────────────────────┐
│              DataEntry<T>                │
├─────────────────────────────────────────┤
│  key: String (UUID)         — unique ID │
│  version: long              — event ver │
│  data: T                    — user data │
│  created: Instant           — timestamp │
│  modified: Instant          — timestamp │
│  request: String            — lock ref  │
├─────────────────────────────────────────┤
│  [transient] tableName: String          │
│  [transient] dataTable: DataTable       │
└─────────────────────────────────────────┘
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `key` | `String` | Auto-generated UUID v4. Primary identifier for the entry. |
| `version` | `long` | Monotonically increasing version counter. Starts at 0. Incremented on each update/delete. Used for event ordering. |
| `data` | `T` | The user-defined data object (e.g., `Author`, `Book`). Serialized to JSON for event sourcing. |
| `created` | `Instant` | Timestamp when the entry was first created. |
| `modified` | `Instant` | Timestamp of the last modification. Updated on `versionIncrease()`. |
| `request` | `String` | Lock reference ID. Set when the entry is copied with a lock for write operations. |
| `tableName` | `String` | (transient) The table this entry belongs to. Set during consumption. |
| `dataTable` | `DataTable` | (transient) Back-reference to the owning DataTable. Used for lock operations in `copy()`. |

### Constructors

```java
// Create new entry with auto-generated UUID
DataEntry(T obj)

// Reconstruct from a Create modification event (during consumption)
DataEntry(Create create)

// Create with specific key (for reconstruction)
DataEntry(String key)

// Default constructor (auto-generated UUID, no data)
DataEntry()
```

### Copy Mechanism

DataEntry provides a `copy()` method that is central to the write workflow:

```java
// Copy without lock (read-only copy)
<T extends DataEntry> T copy(boolean lock)

// Copy with explicit class and optional lock
<T extends DataEntry> T copy(Class<T> clazz, boolean lock)
```

When `lock=true`:
1. Acquires a distributed lock via `LockManager.waitForLock(tableName, key)`
2. Deep-copies the entry via JSON serialization/deserialization
3. Sets the `request` field to the lock reference ID
4. The lock is released when the modified copy is saved back

When `lock=false`:
1. Deep-copies the entry via JSON serialization/deserialization
2. No lock is acquired

### Equality and Ordering

DataEntry equality is based solely on the `key` field:
```java
public boolean equals(Object obj) {
    if (obj instanceof DataEntry) {
        return ((DataEntry) obj).getKey().equals(this.key);
    }
    return super.equals(obj);
}

public int compareTo(DataEntry o) {
    return this.key.compareTo(o.key);
}
```

## Connection\<F, T\>

**Source:** `database/tables/connection/Connection.java`

`Connection<F extends DataEntry, T extends DataEntry>` represents a directed edge between two DataEntries with optional metadata. It links a "from" entry to a "to" entry.

### Structure

```
┌──────────────────────────────────────────┐
│          Connection<F, T>                 │
├──────────────────────────────────────────┤
│  uuid: String               — unique ID  │
│  fromKey: String            — from entry  │
│  toKey: String              — to entry    │
│  version: long              — event ver   │
│  date: Instant              — created     │
│  modified: Instant          — modified    │
│  request: String            — lock ref    │
│  metadata: Map<String,Str>  — key-value   │
├──────────────────────────────────────────┤
│  [transient] connectionHandler            │
└──────────────────────────────────────────┘
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | `String` | Auto-generated UUID v4. Unique identifier for the connection. |
| `fromKey` | `String` | UUID key of the source DataEntry. |
| `toKey` | `String` | UUID key of the target DataEntry. |
| `version` | `long` | Version counter for event ordering. |
| `date` | `Instant` | Creation timestamp. |
| `modified` | `Instant` | Last modification timestamp. |
| `request` | `String` | Lock reference ID for write coordination. |
| `metadata` | `Map<String, String>` | Arbitrary key-value metadata on the edge. |

### Navigation Methods

Connections provide convenience methods to resolve connected entries:

```java
// Get the target DataEntry by looking up toKey in the target table
public T toEntry()

// Get the source DataEntry by looking up fromKey in the source table
public F fromEntry()
```

These methods use the `connectionHandler` back-reference to access the NucleoDB instance and resolve the appropriate tables.

## Annotations

### @Table

**Source:** `database/tables/annotation/Table.java`

Marks a POJO class as a NucleoDB table. Applied to the data class (not the DataEntry subclass).

```java
@Table(tableName = "author", dataEntryClass = AuthorDE.class)
public class Author implements Serializable {
    @Index
    String name;
    // ...
}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `tableName` | `String` | The table name. If empty, defaults to the class simple name in lowercase. |
| `dataEntryClass` | `Class` | The DataEntry subclass that wraps this data class. Must extend `DataEntry`. |

### @Conn

**Source:** `database/tables/annotation/Conn.java`

Marks a Connection subclass as a NucleoDB connection type.

```java
@Conn("AuthorBook")
public class AuthorBookConnection extends Connection<AuthorDE, BookDE> {
    // custom fields...
}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `value` | `String` | The connection label. Used to derive the Kafka topic name (`value.toLowerCase() + "s"`). |

### @Index

**Source:** `database/index/annotation/Index.java`

Marks a field for indexing. Fields annotated with `@Index` are automatically indexed during table initialization.

```java
@Table(tableName = "author", dataEntryClass = AuthorDE.class)
public class Author implements Serializable {
    @Index
    String name;

    @Index(type = TreeIndex.class)
    int age;
}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `value` | `String` | Custom index key path. If empty, uses the field path. |
| `type` | `Class` | Index implementation class. Default is `TrieIndex`. Use `TreeIndex` for numeric/range queries. |

## DataEntry Subclass Requirements

Custom DataEntry subclasses must override specific constructors:

```java
public class AuthorDE extends DataEntry<Author> {

    // Required: default constructor
    public AuthorDE() { super(); }

    // Required: construct from key string
    public AuthorDE(String key) { super(key); }

    // Required: construct from data object
    public AuthorDE(Author author) { super(author); }

    // Required: reconstruct from Create event
    public AuthorDE(Create create) throws ClassNotFoundException, JsonProcessingException {
        super(create);
    }
}
```

The system validates these constructors at startup and throws `MissingDataEntryConstructorsException` if any are missing.

## Model Relationships

```
┌──────────────────┐         ┌──────────────────────┐
│   @Table class   │         │   @Conn class         │
│  (e.g., Author)  │         │  (e.g., AuthorBook)   │
│                  │         │                       │
│  @Index fields   │         │  extends Connection   │
│                  │         │  <AuthorDE, BookDE>   │
└────────┬─────────┘         └───────────┬───────────┘
         │ wrapped by                    │ managed by
         ▼                               ▼
┌──────────────────┐         ┌──────────────────────┐
│ DataEntry<Author>│◄───────►│  ConnectionHandler   │
│   (AuthorDE)     │  edges  │  <AuthorBook>        │
│                  │         │                       │
│  key: UUID       │         │  fromKey ──► AuthorDE │
│  data: Author    │         │  toKey   ──► BookDE   │
│  version: long   │         │  metadata: Map        │
└────────┬─────────┘         └───────────────────────┘
         │ stored in
         ▼
┌──────────────────┐
│   DataTable      │
│  <AuthorDE>      │
│                  │
│  entries (Set)   │
│  indexes (Map)   │
│  keyToEntry (Map)│
└──────────────────┘
```

## Data Entry Projection

**Source:** `database/tables/table/DataEntryProjection.java`

`DataEntryProjection` controls how data is returned from queries:

```java
// Read-only (default) — returns references to live objects
Set<DataEntry> results = table.get("name", "test");

// Writable copy with lock — deep-copies entries and acquires locks
Set<DataEntry> results = table.get("name", "test", new DataEntryProjection() {{
    setWritable(true);
    setLockUntilWrite(true);
}});
```

| Property | Default | Description |
|----------|---------|-------------|
| `writable` | `false` | If true, returns deep copies instead of live references. |
| `lockUntilWrite` | `false` | If true, acquires distributed locks on returned entries. |

## Type Hierarchy

```
Serializable, Comparable<DataEntry>
    └── DataEntry<T>
            └── AuthorDE (user-defined)
            └── BookDE (user-defined)
            └── ... (user-defined)

Serializable, Comparable<Connection>
    └── Connection<F extends DataEntry, T extends DataEntry>
            └── AuthorBookConnection (user-defined)
            └── ... (user-defined)
```
