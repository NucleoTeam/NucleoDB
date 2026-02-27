# Architecture Overview

## Introduction

NucleoDB is an in-memory, event-sourced, embedded database library for Java. It provides high-speed data management by keeping all data in memory while using an event-sourcing backbone (Apache Kafka or local queues) for distributed state synchronization, durability, and replayability. It is designed to be embedded directly into Java applications, eliminating the need for external database processes.

**Key characteristics:**
- In-memory storage for sub-millisecond read latency
- Event-sourced writes via a pluggable message queue system (Kafka by default)
- Graph-like connection model between data entries
- Distributed locking for write coordination
- SQL query support via JSQLParser
- Annotation-driven schema definition with automatic package scanning
- Pluggable indexing (Trie-based and Tree-based)

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         NucleoDB (Facade)                           │
│                                                                     │
│  ┌──────────────┐   ┌───────────────────┐   ┌───────────────────┐  │
│  │  DataTable<T> │   │ ConnectionHandler │   │   LockManager     │  │
│  │  (per @Table) │   │  <C> (per @Conn)  │   │   (singleton)     │  │
│  ├──────────────┤   ├───────────────────┤   ├───────────────────┤  │
│  │ entries (Set) │   │ connections (Map) │   │ activeLocks (Map) │  │
│  │ indexes (Map) │   │ connectionsRev    │   │ waiting (Queue)   │  │
│  │ keyToEntry    │   │ connectionByUUID  │   │ pendingLocks      │  │
│  │ modqueue      │   │ modqueue          │   │                   │  │
│  └──────┬───────┘   └────────┬──────────┘   └────────┬──────────┘  │
│         │                    │                        │             │
│  ┌──────┴────────────────────┴────────────────────────┴──────────┐  │
│  │                   Message Queue System (MQS)                  │  │
│  │  ┌──────────────────┐          ┌─────────────────────┐        │  │
│  │  │  ConsumerHandler │◄────────►│  ProducerHandler    │        │  │
│  │  │  (36 threads)    │          │                     │        │  │
│  │  └────────┬─────────┘          └─────────────────────┘        │  │
│  │           │                                                   │  │
│  │  ┌────────┴─────────┐                                         │  │
│  │  │  QueueHandler    │  (routes messages by type prefix)       │  │
│  │  └──────────────────┘                                         │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────┐  ┌──────────────┐  ┌───────────────────────┐    │
│  │  SaveHandler  │  │ ExportHandler│  │  ModQueueHandler      │    │
│  │  (periodic    │  │ (JSON export │  │  (reorder out-of-seq  │    │
│  │   disk save)  │  │  of mods)    │  │   modifications)      │    │
│  └───────────────┘  └──────────────┘  └───────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴──────────┐
                    │   Apache Kafka     │
                    │   (or LocalMQS)    │
                    │                    │
                    │  topic per table   │
                    │  topic per conn    │
                    │  topic for locks   │
                    └────────────────────┘
```

## Package Structure

```
com.nucleodb.library
├── NucleoDB.java                          # Main facade / entry point
├── database/
│   ├── index/                             # Indexing subsystem
│   │   ├── IndexWrapper.java              # Abstract base for indexes
│   │   ├── TrieIndex.java                 # Trie-based string index
│   │   ├── TreeIndex.java                 # TreeMap-based range index
│   │   ├── annotation/
│   │   │   └── Index.java                 # @Index field annotation
│   │   └── trie/
│   │       ├── Node.java                  # Trie node
│   │       ├── Root.java                  # Trie root array
│   │       └── Entry.java                 # Trie entry wrapper
│   ├── lock/                              # Distributed locking
│   │   ├── LockManager.java              # Lock coordination
│   │   ├── LockConfig.java               # Lock configuration
│   │   └── LockReference.java            # Lock state object
│   ├── modifications/                     # Event sourcing modifications
│   │   ├── Modification.java             # Enum: CREATE, UPDATE, DELETE, CONNECTION*
│   │   ├── Modify.java                   # Base class for all modifications
│   │   ├── Create.java                   # DataEntry create event
│   │   ├── Update.java                   # DataEntry update event (JSON Patch)
│   │   ├── Delete.java                   # DataEntry delete event
│   │   ├── ConnectionCreate.java         # Connection create event
│   │   ├── ConnectionUpdate.java         # Connection update event
│   │   └── ConnectionDelete.java         # Connection delete event
│   ├── tables/
│   │   ├── annotation/
│   │   │   ├── Table.java                # @Table class annotation
│   │   │   └── Conn.java                 # @Conn connection annotation
│   │   ├── table/
│   │   │   ├── DataTable.java            # Core data table engine
│   │   │   ├── DataTableBuilder.java     # Builder pattern for DataTable
│   │   │   ├── DataTableConfig.java      # Table configuration
│   │   │   ├── DataEntry.java            # Generic data entry wrapper
│   │   │   ├── DataEntryProjection.java  # Field projection for reads
│   │   │   ├── SaveHandler.java          # Periodic disk persistence
│   │   │   ├── ExportHandler.java        # JSON modification export
│   │   │   └── ModQueueHandler.java      # Out-of-order mod reprocessor
│   │   └── connection/
│   │       ├── ConnectionHandler.java    # Connection table engine
│   │       ├── Connection.java           # Generic connection/edge
│   │       ├── ConnectionConfig.java     # Connection configuration
│   │       ├── ConnectionProjection.java # Field projection for connections
│   │       ├── SaveHandler.java          # Connection disk persistence
│   │       ├── ExportHandler.java        # Connection JSON export
│   │       └── ModQueueHandler.java      # Connection mod reprocessor
│   └── utils/
│       ├── Serializer.java               # Jackson ObjectMapper management
│       ├── ObjectFileReader.java         # Java serialization reader
│       ├── ObjectFileWriter.java         # Java serialization writer
│       ├── JsonOperations.java           # JSON Patch operation model
│       ├── StartupRun.java              # Startup callback interface
│       └── sql/
│           ├── SQLHandler.java           # SQL parsing and execution
│           └── PrimaryKey.java           # @PrimaryKey annotation
├── event/
│   ├── DataTableEventListener.java       # Table event callback interface
│   └── ConnectionEventListener.java      # Connection event callback interface
├── mqs/                                   # Message Queue System
│   ├── ConsumerHandler.java              # Abstract consumer
│   ├── ProducerHandler.java              # Abstract producer
│   ├── QueueHandler.java                 # Message routing/dispatch
│   ├── config/
│   │   ├── MQSConfiguration.java         # MQS factory configuration
│   │   ├── MQSConstructorSettings.java   # Reflective constructor config
│   │   └── MQSSettings.java             # Abstract MQS settings
│   ├── kafka/
│   │   ├── KafkaConfiguration.java       # Kafka MQS factory
│   │   ├── KafkaConsumerHandler.java     # Kafka consumer implementation
│   │   ├── KafkaProducerHandler.java     # Kafka producer implementation
│   │   └── KafkaMQSSettings.java        # Kafka-specific settings
│   └── local/
│       ├── LocalConfiguration.java       # In-process MQS factory
│       ├── LocalConsumerHandler.java     # In-process consumer
│       ├── LocalProducerHandler.java     # In-process producer
│       └── LocalMQSSettings.java        # Local MQS settings
└── utils/
    ├── EnvReplace.java                   # Environment variable substitution
    └── field/
        └── FieldFinder.java             # Annotation-based field discovery
```

## Core Components

### NucleoDB (Facade)
**Source:** `NucleoDB.java`

The central entry point and orchestrator. Manages the lifecycle of all tables, connections, and the lock manager. Provides:
- Annotation-based package scanning to discover `@Table` and `@Conn` classes
- `CountDownLatch` coordination for startup synchronization
- SQL query interface (`sql()`, `select()`, `insert()`, `update()`)
- Table and connection lookup by name or annotated class

### DataTable\<T\>
**Source:** `database/tables/table/DataTable.java`

The core data storage engine. Each `@Table`-annotated class gets its own `DataTable` instance. Manages:
- In-memory entry set with UUID-based key lookup
- Pluggable index system (`TreeIndex`, `TrieIndex`)
- Event-sourced CRUD via `modify()` with version sequencing
- Consumer/producer for Kafka communication
- Background threads: `SaveHandler`, `ModQueueHandler`, `ExportHandler`

### ConnectionHandler\<C\>
**Source:** `database/tables/connection/ConnectionHandler.java`

Graph-like relationship engine between data entries. Each `@Conn`-annotated class gets its own `ConnectionHandler`. Manages:
- Bidirectional connection indexes (forward and reverse)
- Composite key lookups (fromKey, fromKey+toKey, toKey, toKey+fromKey)
- Same event-sourcing pattern as DataTable with `ConnectionCreate/Update/Delete`

### LockManager
**Source:** `database/lock/LockManager.java`

Distributed locking system using Kafka for lock coordination across nodes. Provides:
- Optimistic lock acquisition with `waitForLock()`
- Automatic lock expiration (1-second timeout)
- Lock queue management for contention resolution

## Database Modes (DBType)

| Mode | Read | Write | Persistence | Export | Use Case |
|------|------|-------|-------------|--------|----------|
| `ALL` | Yes | Yes | Yes | No | Full read/write node |
| `NO_LOCAL` | Yes | Yes | No | No | Stateless node, no disk I/O |
| `READ_ONLY` | Yes | No | Yes | No | Read replica |
| `EXPORT` | Yes | Yes | Yes | Yes | Node with JSON audit log |

## Technology Stack

| Component | Library | Version | Purpose |
|-----------|---------|---------|---------|
| Build | Gradle | - | Build system |
| Language | Java | 19 | Runtime |
| Messaging | Apache Kafka | 2.1.0 | Event sourcing backbone |
| Serialization | Jackson | 2.2.3+ | JSON serialization |
| Diff/Patch | json-patch | 1.13 | RFC 6902 JSON Patch for updates |
| SQL | JSQLParser | 4.6 | SQL query parsing |
| Caching | Guava | 28.1 | Cache, collections, concurrency |
| Reflection | Reflections | 0.10.2 | Package scanning |
| UUID | uuid-creator | 4.5.0 | UUID generation |

## Threading Model

Each `DataTable` and `ConnectionHandler` spawns multiple background threads:

1. **Consumer threads** (36 per table/connection) - `QueueHandler` instances that process messages from the Kafka consumer queue
2. **ModQueueHandler thread** (1 per table/connection) - Reprocesses out-of-order modifications
3. **SaveHandler thread** (1 per table/connection, if persistence enabled) - Periodic disk serialization
4. **ExportHandler thread** (1 per table/connection, if export enabled) - JSON modification export
5. **LockManager thread** (1 global) - Scheduled lock queue processing every 10ms
