# Messaging System (MQS)

## Overview

NucleoDB uses a pluggable Message Queue System (MQS) as the backbone for all event-sourced operations. The MQS layer abstracts the message transport, allowing different implementations to be swapped in. Two implementations are provided: **Kafka** (production) and **Local** (in-process testing).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   MQSConfiguration                   │
│  (Factory for creating Consumer/Producer handlers)   │
├──────────────────────┬──────────────────────────────┤
│                      │                               │
│  ┌───────────────┐   │   ┌───────────────────────┐  │
│  │ ConsumerHandler│   │   │  ProducerHandler      │  │
│  │ (abstract)    │   │   │  (abstract)            │  │
│  └───────┬───────┘   │   └──────────┬────────────┘  │
│          │           │              │                │
│  ┌───────┴───────┐   │   ┌──────────┴────────────┐  │
│  │QueueHandler   │   │   │                       │  │
│  │(36 threads)   │   │   │                       │  │
│  └───────────────┘   │   │                       │  │
└──────────────────────┴───┴───────────────────────┘  │
                                                       │
          ┌────────────────┬───────────────────────────┘
          │                │
          ▼                ▼
┌─────────────────┐  ┌─────────────────┐
│ KafkaConsumer   │  │ LocalConsumer   │
│ Handler         │  │ Handler         │
│                 │  │                 │
│ KafkaProducer   │  │ LocalProducer   │
│ Handler         │  │ Handler         │
└─────────────────┘  └─────────────────┘
```

## MQSConfiguration

**Source:** `mqs/config/MQSConfiguration.java`

The central factory that creates ConsumerHandler and ProducerHandler instances via reflection.

### Construction

```java
MQSConfiguration(
    MQSConstructorSettings<? extends ConsumerHandler> consumer,  // consumer class + constructor params
    MQSConstructorSettings<? extends ProducerHandler> producer,  // producer class + constructor params
    Class<? extends MQSSettings> settingClass                    // settings class
)
```

### Factory Methods

```java
// Creates a ConsumerHandler using reflection
ConsumerHandler createConsumerHandler(Map<String, Object> settingsMap)

// Creates a ProducerHandler using reflection
ProducerHandler createProducerHandler(Map<String, Object> settingsMap)
```

Both methods:
1. Instantiate the settings class with the settings map
2. Use `PropertyDescriptor` to read constructor parameters from settings
3. Reflectively invoke the handler constructor

## ConsumerHandler

**Source:** `mqs/ConsumerHandler.java`

Abstract base for message consumers. Subclasses must override `run()` to implement the actual message polling.

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `database` | `DataTable` | Set when consuming for a DataTable |
| `connectionHandler` | `ConnectionHandler` | Set when consuming for connections |
| `lockManager` | `LockManager` | Set when consuming for lock operations |
| `queue` | `Queue<String>` | In-memory message queue for QueueHandler threads |
| `leftToRead` | `AtomicInteger` | Count of messages waiting to be processed |
| `startupPhaseConsume` | `AtomicBoolean` | True during initial catch-up from Kafka |
| `startupLoadCount` | `AtomicInteger` | Count of events during startup phase |
| `topic` | `String` | Kafka topic name |

### Thread Model

```java
public void start(int queues) {
    for (int x = 0; x < queues; x++) {
        Thread queueThread = new Thread(new QueueHandler(this));
        queueTasks.submit(queueThread);
    }
}
```

Each ConsumerHandler starts **36 QueueHandler threads** (passed as parameter). The ConsumerHandler itself runs in a separate thread (the Kafka polling loop). The QueueHandler threads pull from the shared in-memory `queue`.

### Message Flow

```
Kafka/Local Topic
      │
      ▼
ConsumerHandler.run()    (1 thread — Kafka poll loop)
      │
      │ message received
      │ queue.add(message)
      │ leftToRead.incrementAndGet()
      │
      ▼
QueueHandler.run()       (36 threads — parallel processing)
      │
      │ queue.poll()
      │ leftToRead.decrementAndGet()
      │
      ├─── DataTable type?  → dataTableType(entry)
      ├─── Connection type? → connectionType(entry)
      └─── Lock type?       → lockManager.lockAction(entry)
```

## ProducerHandler

**Source:** `mqs/ProducerHandler.java`

Abstract base for message producers. Subclasses must override both `push()` methods.

### Methods

```java
// Push a modification event (for DataTable and Connection operations)
void push(String key, long version, Modify modify, Callback callback)

// Push a raw string message (for lock operations)
void push(String key, String message)
```

## QueueHandler (Message Router)

**Source:** `mqs/QueueHandler.java`

The internal message processor that routes consumed messages to the appropriate handler.

### Message Format

Messages are type-prefixed strings:

| Type | Prefix Length | Prefix Value | Target |
|------|--------------|-------------|--------|
| DataTable Create | 6 chars | `CREATE` | `DataTable.modify()` |
| DataTable Update | 6 chars | `UPDATE` | `DataTable.modify()` |
| DataTable Delete | 6 chars | `DELETE` | `DataTable.modify()` |
| Connection Create | 16 chars | `CONNECTIONCREATE` | `ConnectionHandler.modify()` |
| Connection Update | 16 chars | `CONNECTIONUPDATE` | `ConnectionHandler.modify()` |
| Connection Delete | 16 chars | `CONNECTIONDELETE` | `ConnectionHandler.modify()` |
| Lock Reference | Full JSON | N/A | `LockManager.lockAction()` |

### Routing Logic

```java
// In QueueHandler.run()
if (databaseType) {
    dataTableType(entry);     // Parse 6-char prefix
} else if (connectionType) {
    connectionType(entry);    // Parse 16-char prefix
} else if (lockdownType) {
    lockManager.lockAction(   // Deserialize full LockReference
        Serializer.getObjectMapper().getOm().readValue(entry, LockReference.class)
    );
}
```

### DataTable Message Processing

```java
private void dataTableType(String entry) {
    String type = entry.substring(0, 6);    // "CREATE", "UPDATE", or "DELETE"
    String data = entry.substring(6);        // JSON payload
    Modification mod = Modification.get(type);
    database.modify(mod, Serializer.getObjectMapper().getOm().readValue(data, mod.getModification()));
}
```

### Connection Message Processing

```java
private void connectionType(String entry) {
    String type = entry.substring(0, 16);   // "CONNECTIONCREATE", etc.
    String data = entry.substring(16);       // JSON payload
    Modification mod = Modification.get(type);
    connectionHandler.modify(mod, Serializer.getObjectMapper().getOm().readValue(data, mod.getModification()));
}
```

## Kafka Implementation

### KafkaConfiguration

**Source:** `mqs/kafka/KafkaConfiguration.java`

Factory that creates `KafkaConsumerHandler` and `KafkaProducerHandler` instances.

### KafkaConsumerHandler

**Source:** `mqs/kafka/KafkaConsumerHandler.java`

Implements the Kafka polling loop:

```
┌──────────────────────────────────────────┐
│         KafkaConsumerHandler             │
│                                          │
│  1. Subscribe to topic                   │
│  2. Seek to saved partition offsets      │
│     (from DataTable.partitionOffsets)    │
│  3. Poll loop:                           │
│     a. consumer.poll(Duration)           │
│     b. For each record:                  │
│        - queue.add(record.value())       │
│        - leftToRead.incrementAndGet()    │
│        - Track partition offsets         │
│     c. Notify queue threads              │
│  4. Detect end of startup phase          │
│     when poll returns empty              │
└──────────────────────────────────────────┘
```

Key behaviors:
- **Partition offset tracking**: Stores the latest offset per partition in `partitionOffsets` (saved to disk by SaveHandler)
- **Startup phase**: During startup, all existing messages are consumed. When a poll returns zero records, `startupPhaseConsume` is set to `false`, signaling that catch-up is complete
- **Consumer group**: Each node creates its own consumer group (using `consumerId`) to ensure all nodes receive all messages

### KafkaProducerHandler

**Source:** `mqs/kafka/KafkaProducerHandler.java`

Publishes modifications to Kafka:

```java
// For DataTable/Connection modifications
void push(String key, long version, Modify modify, Callback callback) {
    String prefix = modify.getClass().getSimpleName().toUpperCase();  // e.g., "CREATE"
    String payload = Serializer.getObjectMapper().getOm().writeValueAsString(modify);
    String message = prefix + payload;
    producer.send(new ProducerRecord<>(topic, key, message), callback);
}

// For lock operations
void push(String key, String message) {
    producer.send(new ProducerRecord<>(topic, key, message));
}
```

### Kafka Topic Structure

Each table, connection type, and the lock manager gets its own Kafka topic:

| Entity | Topic Name | Example |
|--------|-----------|---------|
| `@Table(tableName="author")` | `author` | Messages: `CREATE{...}`, `UPDATE{...}`, `DELETE{...}` |
| `@Conn("AuthorBook")` | `authorbooks` | Messages: `CONNECTIONCREATE{...}`, etc. |
| LockManager | `locks` (configurable) | Messages: `{LockReference JSON}` |

## Local Implementation

### LocalConfiguration

**Source:** `mqs/local/LocalConfiguration.java`

Factory for in-process message passing. Useful for testing and single-node deployments.

### LocalConsumerHandler / LocalProducerHandler

Instead of Kafka, messages are passed through an in-memory queue within the same JVM process:

```
LocalProducerHandler.push()
      │
      │ Add to shared in-memory queue
      │
      ▼
LocalConsumerHandler.run()
      │
      │ Poll from shared queue
      │ Add to ConsumerHandler.queue
      │
      ▼
QueueHandler.run()
      │
      │ (same processing as Kafka)
```

## Settings Map

Both DataTable and ConnectionHandler pass a `Map<String, Object>` to the MQS layer:

| Key | Type | Description |
|-----|------|-------------|
| `topic` | `String` | Kafka topic name |
| `bootstrap` | `String` | Kafka bootstrap servers |
| `consumerHandler` | `ConsumerHandler` | Back-reference (set after creation) |
| (custom) | varies | Implementation-specific settings |

## Startup Phase Detection

The consumer startup phase is critical for correctness:

```
Phase 1: STARTUP (startupPhaseConsume = true)
├── All existing Kafka messages are consumed
├── startupLoadCount tracks events pending processing
├── Events are processed but startup callback is deferred
│
Phase 2: TRANSITION (poll returns empty)
├── startupPhaseConsume set to false
├── When startupLoadCount reaches 0:
│   └── startup() callback fires (CountDownLatch.countDown())
│
Phase 3: RUNTIME (startupPhase = false)
├── Normal real-time event processing
├── Event listeners fire immediately
└── Garbage collection triggered after startup
```

## Reload Mechanism

DataTable and ConnectionHandler support reloading from Kafka:

```java
public boolean reload(Consumer reloadComplete) {
    if (runningReload != null && !runningReload.isDone()) return false;
    runningReload = reloadExecutor.submit(this.consumer.reload(reloadComplete));
    return true;
}
```

This creates a new consumer that re-reads the topic from the beginning, allowing the in-memory state to be rebuilt.
