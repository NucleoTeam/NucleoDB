# Distributed Locking

## Overview

NucleoDB includes a distributed locking system (`LockManager`) that coordinates write access to individual data entries across multiple nodes. It uses the same Kafka messaging backbone as the data tables to ensure all nodes agree on lock ownership. The locking system prevents concurrent modifications to the same entry, which could cause version conflicts.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    LockManager                        │
│                                                       │
│  ownerId: UUID         ← unique per NucleoDB instance │
│                                                       │
│  ┌──────────────┐  ┌───────────────┐                 │
│  │ activeLocks   │  │ pendingLocks  │                 │
│  │ ConcurrentMap │  │ Map           │                 │
│  │               │  │               │                 │
│  │ key: table_id │  │ key: request  │                 │
│  │ val: LockRef  │  │ val: LockRef  │                 │
│  └──────────────┘  └───────────────┘                 │
│                                                       │
│  ┌──────────────┐  ┌───────────────┐                 │
│  │ waiting       │  │ queue         │                 │
│  │ ConcurrentMap │  │ LinkedBlocking│                 │
│  │               │  │ Queue         │                 │
│  │ key: table_id │  │               │                 │
│  │ val: Queue of │  │ LockReference │                 │
│  │   LockRefs   │  │ objects       │                 │
│  └──────────────┘  └───────────────┘                 │
│                                                       │
│  ┌──────────────────────────────────────────┐        │
│  │  Kafka: locks topic                       │        │
│  │  ConsumerHandler ◄──► ProducerHandler     │        │
│  └──────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────┘
```

## LockReference

The `LockReference` object represents a lock request or active lock:

| Field | Type | Description |
|-------|------|-------------|
| `tableName` | `String` | Table or connection label |
| `key` | `String` | Entry UUID being locked |
| `owner` | `String` | NucleoDB instance `ownerId` |
| `request` | `String` | Unique lock request ID |
| `lock` | `boolean` | `true` = acquire, `false` = release |
| `time` | `Instant` | Timestamp of the lock action |

The composite key for lock identification is `tableName_key` (e.g., `author_abc-123`).

## Lock Acquisition Flow

### Step-by-Step

```
Node A: User calls entry.copy(true)
    │
    ▼
LockManager.waitForLock("author", "abc-123")
    │
    │ 1. Create LockReference(table, key, ownerId, lock=true)
    │ 2. Add to queue
    │ 3. synchronized(lockReference) { lockReference.wait() }
    │    (thread blocks)
    │
    ▼
LockManager.run() — scheduled every 10ms
    │
    │ 4. Poll from queue
    │ 5. Store in pendingLocks[request]
    │ 6. Add to waiting[table_key] queue
    │ 7. Push LockReference to Kafka locks topic
    │
    ▼
Kafka: locks topic
    │
    │ 8. Delivered to ALL nodes (including self)
    │
    ▼
ConsumerHandler → QueueHandler
    │
    │ 9. Deserialize to LockReference
    │ 10. Call lockManager.lockAction(lockReference)
    │
    ▼
LockManager.lockAction()
    │
    │ 11. lockReference.isLock() == true
    │ 12. Check activeLocks for table_key
    │     ├── No active lock → Grant immediately
    │     └── Active lock exists:
    │         ├── Same request? → Grant (idempotent)
    │         └── Different request?
    │             └── Active lock expired (>2s)? → Grant
    │             └── Active lock fresh? → Do not grant (wait)
    │
    │ 13. If granted:
    │     a. activeLocks.put(table_key, lockReference)
    │     b. Remove from waiting queue
    │     c. Lookup pendingLocks[request]
    │     d. synchronized(pendingLockRef) { pendingLockRef.notify() }
    │        (unblocks the waiting thread on THIS node)
    │
    ▼
Thread unblocked
    │
    │ 14. Return LockReference to caller
    │ 15. Set entry.request = lockReference.request
    │
    ▼
Entry copy returned with lock
```

### Sequence Diagram

```
Node A (Requester)              Kafka               All Nodes
    │                              │                     │
    │ waitForLock(table, key)      │                     │
    │──► queue.add(lockRef)        │                     │
    │    thread.wait()             │                     │
    │                              │                     │
    │    (10ms scheduler)          │                     │
    │──► push(lockRef) ───────────►│                     │
    │                              │────────────────────►│
    │                              │                     │
    │                              │  lockAction()       │
    │                              │  ┌───────────────┐  │
    │                              │  │ No active lock│  │
    │                              │  │ → Grant lock  │  │
    │                              │  │ activeLocks   │  │
    │                              │  │  .put(key,ref)│  │
    │                              │  └───────────────┘  │
    │                              │                     │
    │  pendingLock.notify() ◄──────┤─────────────────────┤
    │  (unblocked)                 │                     │
    │                              │                     │
    │  return LockReference        │                     │
    │                              │                     │
```

## Lock Release Flow

Locks are released in two ways:

### 1. Explicit Release (after modification is consumed)

When a modification is consumed and applied, `consumerResponse()` triggers lock release:

```java
// In DataTable.consumerResponse()
if (T != null) {
    getNucleoDB().getLockManager().releaseLock(
        this.config.getTable(),
        T.getKey(),
        T.getRequest()   // Lock request ID
    );
}
```

```
DataTable.modify() completes
    │
    ▼
consumerResponse(entry, changeUUID)
    │
    ▼
LockManager.releaseLock(table, key, request)
    │
    │ 1. Lookup activeLocks[table_key]
    │ 2. Verify request matches active lock
    │ 3. Verify owner matches ownerId
    │ 4. Create release LockReference (lock=false)
    │ 5. Push to Kafka
    │
    ▼
Kafka → All Nodes → lockAction()
    │
    │ 6. lockReference.isLock() == false
    │ 7. Verify request matches current active lock
    │ 8. activeLocks.remove(table_key)
    │ 9. Check waiting queue for next lock request
    │ 10. If next request exists:
    │     push(nextLockRef) to Kafka → Cycle repeats
    │
    ▼
Lock released, next waiter (if any) is granted
```

### 2. Automatic Expiration

The `activeLocks` ConcurrentMap overrides `put()` to schedule automatic expiration:

```java
activeLocks = new ConcurrentHashMap() {
    @Override
    public Object put(Object key, Object value) {
        executorPool.schedule(() -> {
            LockReference lockReference = (LockReference) this.get(key);
            if (lockReference != null && lockReference.getRequest().equals(
                ((LockReference)value).getRequest())) {
                // Lock still held by same request after 1000ms → force release
                LockReference releaseLock = copy(lockReference);
                releaseLock.setLock(false);
                lockAction(releaseLock);
            }
        }, 1000, TimeUnit.MILLISECONDS);
        return super.put(key, value);
    }
};
```

Every time a lock is stored, a 1-second delayed task is scheduled. If the lock is still held by the same request when the timer fires, it is automatically released. This prevents deadlocks from crashed nodes.

## Lock Contention Handling

When multiple nodes compete for the same lock:

```
Node A: waitForLock(table, key1)  ──► push(lockA) to Kafka
Node B: waitForLock(table, key1)  ──► push(lockB) to Kafka

Kafka delivers lockA first to all nodes:
  All nodes: lockAction(lockA) → granted
  activeLocks[table_key1] = lockA

Kafka delivers lockB to all nodes:
  All nodes: lockAction(lockB)
  → Active lock exists (lockA), not expired
  → Do nothing (lockB stays in waiting queue)

Node A completes modification:
  releaseLock(table, key1, lockA.request)
  → Push release to Kafka

Kafka delivers release:
  All nodes: lockAction(release)
  → Remove lockA from activeLocks
  → Check waiting queue → find lockB
  → Push lockB to Kafka

Kafka delivers lockB:
  All nodes: lockAction(lockB) → granted
  Node B: pendingLock.notify() → unblocked
```

## Lock Validation Methods

```java
// Check if entry is available (no active lock or lock expired)
public boolean availableForLock(String table, String key)

// Check if THIS node holds the lock for a specific request
public boolean hasLock(String table, String key, String request)
```

### availableForLock

```java
public boolean availableForLock(String table, String key) {
    LockReference ifPresent = activeLocks.get(table + "_" + key);
    if (ifPresent == null) return true;
    if (!ifPresent.getTime().plusSeconds(1).isAfter(Instant.now())) return true;
    return false;
}
```

### hasLock

```java
public boolean hasLock(String table, String key, String request) {
    LockReference ifPresent = activeLocks.get(table + "_" + key);
    if (ifPresent == null) return false;
    if (!ifPresent.getRequest().equals(request)) return false;
    if (!ifPresent.getOwner().equals(ownerId)) return false;
    if (!ifPresent.getTime().plusSeconds(1).isAfter(Instant.now())) return false;
    return true;
}
```

## Integration with DataEntry and Connection

### DataEntry.copy(lock=true)

```java
public <T extends DataEntry> T copy(boolean lock) {
    if (lock) {
        LockReference lockReference = this.dataTable.getNucleoDB()
            .getLockManager().waitForLock(this.tableName, key);
        T obj = deepCopy(this);
        obj.setRequest(lockReference.getRequest());
        return obj;
    }
    return deepCopy(this);
}
```

### Connection.copy(clazz, lock=true)

```java
public <T extends Connection> T copy(Class<T> clazz, boolean lock) {
    if (lock) {
        LockReference lockReference = this.connectionHandler.getNucleoDB()
            .getLockManager().waitForLock(
                this.connectionHandler.getConfig().getLabel(),
                uuid
            );
        T obj = deepCopy(this, clazz);
        obj.setRequest(lockReference.getRequest());
        return obj;
    }
    return deepCopy(this, clazz);
}
```

## LockManager Startup

```java
public void startLockManager(Consumer<LockConfig> customizer) {
    LockConfig config = new LockConfig();
    CountDownLatch lockManagerStartupComplete = new CountDownLatch(1);
    config.setStartupRun(lockManager -> lockManagerStartupComplete.countDown());

    if (customizer != null) customizer.accept(config);

    lockManager = new LockManager(config);
    new Thread(lockManager).start();          // Start the 10ms scheduler
    lockManagerStartupComplete.await();        // Wait for consumer to be ready
}
```

The LockManager must be fully operational before any tables or connections start, as they may need locks during startup.

## Configuration

The `LockConfig` class provides:

| Setting | Description |
|---------|-------------|
| `mqsConfiguration` | MQS factory (defaults to Kafka) |
| `settingsMap` | MQS settings (bootstrap servers, topic, etc.) |
| `startupRun` | Callback when LockManager is ready |

## Timing and Guarantees

| Property | Value | Description |
|----------|-------|-------------|
| Lock polling interval | 10ms | How often the queue is checked |
| Lock timeout | 1 second | Auto-release if not explicitly released |
| Lock contention timeout | 2 seconds | How long an active lock blocks others |
| Callback TTL | 5 seconds | How long changeListeners cache entries |

**Note**: The lock system provides optimistic concurrency control. A lock is advisory — it coordinates willing participants but does not prevent a rogue node from writing without acquiring a lock first.
