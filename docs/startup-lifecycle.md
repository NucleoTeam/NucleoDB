# Startup Lifecycle

## Overview

NucleoDB's startup is a carefully coordinated process that initializes the lock manager, discovers annotated data models via package scanning, builds tables and connections, loads saved state, replays events from Kafka, and signals readiness via `CountDownLatch` synchronization. This document details the complete boot sequence.

## High-Level Startup Sequence

```
┌──────────────────────────────────────────────────────────────────┐
│                    NucleoDB Constructor                            │
│                                                                    │
│  1. ─── startLockManager()          ◄── blocks until ready        │
│  2. ─── startTables()               ◄── returns CountDownLatch    │
│  3. ─── startConnections()          ◄── returns CountDownLatch    │
│  4. ─── waitTillReady()             ◄── blocks until all latches  │
│                                          count down               │
└──────────────────────────────────────────────────────────────────┘
```

## Phase 1: Lock Manager Startup

**Source:** `NucleoDB.startLockManager()`

```
startLockManager(customizer)
    │
    │ 1. Create LockConfig with startup callback
    │ 2. Apply customizer (if provided)
    │ 3. Create LockManager(config)
    │     ├── Create Kafka ConsumerHandler for locks topic
    │     ├── Set lockManager on consumer
    │     ├── consumer.start(1)  ← 1 QueueHandler thread for locks
    │     ├── Create Kafka ProducerHandler for locks topic
    │     └── (Consumer starts polling Kafka)
    │ 4. Start LockManager thread
    │     └── Schedules 10ms repeating task for queue processing
    │ 5. lockManagerStartupComplete.await()
    │     └── Blocks until startup callback fires
    │ 6. Ready!
    │
    ▼
LockManager is operational
```

The lock manager must be fully ready before tables and connections start, as they may need locks during startup data loading.

## Phase 2: Table Discovery and Startup

**Source:** `NucleoDB.startTables()`

### Package Scanning

```
startTables(packagesToScan, dbType, readToTime, customizer)
    │
    │ 1. For each package in packagesToScan:
    │     └── new Reflections(package)
    │             .getTypesAnnotatedWith(Table.class)
    │
    │ 2. Collect all @Table-annotated classes
    │
    │ 3. Create CountDownLatch(tableTypes.size())
    │
    ▼
For each @Table class...
```

### Table Initialization

```
For each @Table-annotated class:
    │
    │ 1. Read @Table annotation
    │     ├── tableName (or class.getSimpleName().toLowerCase())
    │     └── dataEntryClass
    │
    │ 2. Validate dataEntryClass extends DataEntry
    │
    │ 3. Validate required constructors:
    │     ├── DataEntryClass()                    — default
    │     ├── DataEntryClass(String key)          — key constructor
    │     ├── DataEntryClass(DataClass data)      — data constructor
    │     └── DataEntryClass(Create create)       — event constructor
    │
    │ 4. Discover @Index fields
    │     └── processIndexListForClass(type) → Set<IndexConfig>
    │
    │ 5. Build DataTableBuilder based on DBType:
    │     ├── ALL       → launchTable()
    │     ├── NO_LOCAL  → launchLocalOnlyTable()  (loadSave=false, saveChanges=false)
    │     ├── READ_ONLY → launchReadOnlyTable()   (write=false)
    │     └── EXPORT    → launchExportOnlyTable() (jsonExport=true)
    │
    │ 6. Configure startup callback:
    │     └── latch.countDown() + fire tableEvents
    │
    │ 7. Apply readToTime (if provided)
    │
    │ 8. Add indexes to builder
    │
    │ 9. DataTableBuilder.build() → DataTable
    │
    ▼
DataTable construction begins...
```

### DataTable Constructor

**Source:** `DataTable(DataTableConfig config)`

```
DataTable(config)
    │
    │ 1. Initialize index wrappers:
    │     For each IndexConfig:
    │         IndexWrapper instance = indexType.newInstance(name)
    │         indexes.put(indexedKey, instance)
    │
    │ 2. Load saved data (if config.loadSave):
    │     loadSavedData()
    │     ├── Read .dat file via ObjectFileReader
    │     ├── Restore entries, keyToEntry, partitionOffsets
    │     ├── Rebuild all indexes from entries
    │     └── Set dataTable back-reference on each entry
    │
    │ 3. Discover fields for the data class
    │
    │ 4. Start Kafka consumer:
    │     startRootConsumer()
    │     ├── Create ConsumerHandler via MQSConfiguration
    │     ├── consumer.setDatabase(this)
    │     ├── consumer.start(36)  ← 36 QueueHandler threads
    │     └── Create ProducerHandler (if write enabled)
    │
    │ 5. Start ModQueueHandler (if read enabled):
    │     new Thread(new ModQueueHandler(this)).start()
    │
    │ 6. Start SaveHandler (if saveChanges enabled):
    │     new Thread(new SaveHandler(this)).start()
    │
    │ 7. Start ExportHandler (if jsonExport enabled):
    │     new Thread(new ExportHandler(this)).start()
    │
    ▼
DataTable is consuming events from Kafka...
```

## Phase 3: Connection Discovery and Startup

**Source:** `NucleoDB.startConnections()`

### Package Scanning

```
startConnections(packagesToScan, dbType, readToTime, customizer)
    │
    │ 1. For each package:
    │     └── new Reflections(package)
    │             .getTypesAnnotatedWith(Conn.class)
    │
    │ 2. Collect all @Conn-annotated classes
    │
    │ 3. Create CountDownLatch(connectionTypes.size())
    │
    ▼
For each @Conn class...
```

### Connection Type Resolution

```
For each @Conn-annotated class:
    │
    │ 1. Read @Conn annotation → label
    │
    │ 2. Derive topic name: label.toLowerCase() + "s"
    │
    │ 3. Resolve generic type parameters:
    │     Connection<FromDE, ToDE>
    │     ├── FromDE → resolve to data class → config.fromTable
    │     └── ToDE  → resolve to data class → config.toTable
    │
    │ 4. Create ConnectionConfig
    │     ├── topic, label, fromTable, toTable
    │     ├── connectionClass
    │     └── readToTime (if provided)
    │
    │ 5. Apply customizer (if provided)
    │
    │ 6. Configure startup callback:
    │     └── latch.countDown() + fire connectionEvents
    │
    │ 7. Apply DBType flags:
    │     ├── NO_LOCAL  → saveChanges=false, loadSaved=false
    │     ├── READ_ONLY → write=false
    │     └── EXPORT    → jsonExport=true
    │
    │ 8. Create ConnectionHandler(nucleoDB, config)
    │
    ▼
ConnectionHandler construction begins...
```

### ConnectionHandler Constructor

**Source:** `ConnectionHandler(NucleoDB nucleoDB, ConnectionConfig config)`

```
ConnectionHandler(nucleoDB, config)
    │
    │ 1. Discover connection class fields
    │
    │ 2. Load saved data (if config.loadSaved):
    │     loadSavedData()
    │     ├── Read .dat file via ObjectFileReader
    │     ├── Restore allConnections via addConnection()
    │     │   (rebuilds forward/reverse index maps)
    │     ├── Restore partitionOffsets
    │     └── Restore consumerId
    │
    │ 3. Start ModQueueHandler (if read enabled):
    │     new Thread(new ModQueueHandler(this)).start()
    │
    │ 4. Start Kafka consumer:
    │     consume()
    │     ├── Create ConsumerHandler via MQSConfiguration
    │     ├── consumer.setConnectionHandler(this)
    │     ├── consumer.start(36)  ← 36 QueueHandler threads
    │     └── Create ProducerHandler (if write enabled)
    │
    │ 5. Start SaveHandler (if saveChanges enabled)
    │
    │ 6. Start ExportHandler (if jsonExport enabled)
    │
    ▼
ConnectionHandler is consuming events from Kafka...
```

## Phase 4: Kafka Catch-Up (Startup Phase)

Once consumers start, they enter the **startup phase** where they catch up on all existing Kafka messages.

```
KafkaConsumerHandler.run()
    │
    │ Phase: STARTUP (startupPhaseConsume = true)
    │
    │ loop:
    │   records = consumer.poll()
    │   if records.isEmpty():
    │     startupPhaseConsume.set(false)     ← Transition signal
    │     break
    │   else:
    │     for each record:
    │       queue.add(record)
    │       startupLoadCount.incrementAndGet()
    │       leftToRead.incrementAndGet()
    │
    │ Phase: RUNTIME
    │
    │ loop:
    │   records = consumer.poll()
    │   for each record:
    │     queue.add(record)
    │     leftToRead.incrementAndGet()
    │
    ▼
Events flow to QueueHandler → DataTable.modify() / ConnectionHandler.modify()
```

### Startup Completion Detection

Each DataTable/ConnectionHandler tracks when the startup phase completes:

```
DataTable.itemProcessed()
    │
    │ Called after each modify() during startup
    │
    │ startupLoadCount.decrementAndGet()
    │
    │ if !consumer.startupPhaseConsume    ← Kafka done sending
    │    && startupLoadCount <= 0:         ← All items processed
    │
    │   startupPhase.set(false)
    │   System.gc()                        ← Clean up temp objects
    │   new Thread(() -> this.startup())   ← Fire startup callbacks
    │
    ▼
DataTable.startup()
    │
    │ inStartup = false
    │ Execute all StartupRun callbacks:
    │   └── latch.countDown()              ← Signal to NucleoDB
    │       tableEvents.forEach(fire)       ← Notify table listeners
```

## Phase 5: Ready Signal

**Source:** `NucleoDB.waitTillReady()`

```java
public void waitTillReady() throws InterruptedException {
    CountDownLatch c;
    while ((c = getLatches().poll()) != null) {
        c.await();
    }
}
```

The caller blocks until all CountDownLatches (one per table, one per connection) have counted down.

## Complete Startup Timeline

```
Time ──────────────────────────────────────────────────────►

     ┌────────────┐
     │ LockManager│
     │   startup  │
     └─────┬──────┘
           │
           │  ┌──────────────────────────────────────────┐
           │  │ Table 1: Load saved → Consume Kafka →    │
           │  │   Process events → startup() → latch ↓   │
           │  └──────────────────────────────────────────┘
           │
           │  ┌──────────────────────────────────────────┐
           │  │ Table 2: Load saved → Consume Kafka →    │
           │  │   Process events → startup() → latch ↓   │
           │  └──────────────────────────────────────────┘
           │
           │  ┌──────────────────────────────────────────┐
           │  │ Conn 1: Load saved → Consume Kafka →     │
           │  │   Process events → startup() → latch ↓   │
           │  └──────────────────────────────────────────┘
           │
           │                                              │
           │◄── Tables + Connections start in parallel ──►│
           │                                              │
           │                                              ▼
           │                                    ┌──────────────┐
           │                                    │ All latches   │
           │                                    │ counted down  │
           │                                    │               │
           │                                    │ waitTillReady │
           │                                    │ returns       │
           │                                    └──────────────┘
```

## Constructor Variants

The `NucleoDB` class provides multiple constructors for different use cases:

### Basic (Full Mode)

```java
// Scan packages, ALL mode, no customization
NucleoDB db = new NucleoDB("com.myapp.models");
```

### With DBType

```java
// Scan packages with specific mode
NucleoDB db = new NucleoDB(DBType.READ_ONLY, "com.myapp.models");
```

### With Time Travel

```java
// Read state as of a specific timestamp
NucleoDB db = new NucleoDB(DBType.ALL, "2024-01-01T00:00:00Z", "com.myapp.models");
```

### With Full Customization

```java
NucleoDB db = new NucleoDB(
    DBType.ALL,
    "2024-01-01T00:00:00Z",
    connectionConsumer -> { /* customize connection config */ },
    dataTableConsumer -> { /* customize table config */ },
    lockConfig -> { /* customize lock config */ },
    "com.myapp.models"
);
```

### Lock Manager Only

```java
// Only start the lock manager (for custom table management)
NucleoDB db = new NucleoDB(lockCustomizer);
```

## Individual Table/Connection Startup

Tables and connections can also be started individually after the NucleoDB instance is created:

```java
// Start a single table
CountDownLatch latch = nucleoDB.startTable(
    MyModel.class,
    DBType.ALL,
    null,           // readToTime
    customizer      // DataTableConsumer
);
latch.await();

// Start a single connection
CountDownLatch latch = nucleoDB.startConnection(
    MyConnection.class,
    DBType.ALL,
    null,           // readToTime
    customizer      // ConnectionConsumer
);
latch.await();
```

## Thread Summary at Startup

For a NucleoDB instance with 2 tables and 1 connection:

| Component | Threads | Purpose |
|-----------|---------|---------|
| LockManager consumer | 1 | Kafka poll loop |
| LockManager QueueHandler | 1 | Lock message processing |
| LockManager scheduler | 1 | 10ms queue processing |
| LockManager executor pool | 1000 | Lock expiration timers |
| Table 1 consumer | 1 | Kafka poll loop |
| Table 1 QueueHandlers | 36 | Message processing |
| Table 1 ModQueueHandler | 1 | Reorder out-of-sequence |
| Table 1 SaveHandler | 1 | Periodic disk save |
| Table 2 consumer | 1 | Kafka poll loop |
| Table 2 QueueHandlers | 36 | Message processing |
| Table 2 ModQueueHandler | 1 | Reorder out-of-sequence |
| Table 2 SaveHandler | 1 | Periodic disk save |
| Conn 1 consumer | 1 | Kafka poll loop |
| Conn 1 QueueHandlers | 36 | Message processing |
| Conn 1 ModQueueHandler | 1 | Reorder out-of-sequence |
| Conn 1 SaveHandler | 1 | Periodic disk save |
| **Total** | **~1120** | |

## Environment Variable Support

Package names support environment variable substitution:

```java
// In NucleoDB.getTableClasses()
new Reflections(replaceEnvVariables(packageToScan))
    .getTypesAnnotatedWith(Table.class)
```

The `EnvReplace.replaceEnvVariables()` utility replaces `${ENV_VAR}` patterns with their environment variable values.

## Error Handling During Startup

| Error | Exception | Behavior |
|-------|-----------|----------|
| DataEntry class doesn't extend DataEntry | `IncorrectDataEntryClassException` | Throws, startup fails |
| Missing required constructors | `MissingDataEntryConstructorsException` | Throws, startup fails |
| Connection missing from/to types | N/A | `System.exit(1)` |
| Invalid readToTime format | `DateTimeParseException` | Logged, continues without time filter |
| Saved data file corrupt | `IOException` / `ClassNotFoundException` | RuntimeException, startup fails |
