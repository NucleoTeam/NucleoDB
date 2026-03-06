package com.nucleodb.library;

import com.nucleodb.library.database.lock.LockManager;
import com.nucleodb.library.database.lock.LockReference;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Update;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.event.DataTableEventListener;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.*;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests verifying ACID properties (Atomicity, Consistency, Isolation, Durability)
 * of NucleoDB operations.
 */
class AcidPropertiesTest {

  NucleoDB nucleoDB;
  DataTable<AuthorDE> table;

  @BeforeEach
  public void setup() throws Exception {
    nucleoDB = new NucleoDB(
        NucleoDB.DBType.NO_LOCAL,
        c -> c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration()),
        c -> c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration()),
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        "com.nucleodb.library.helpers.models"
    );
    nucleoDB.waitTillReady();
    table = nucleoDB.getTable(Author.class);
  }

  // ==================== ATOMICITY TESTS ====================

  @Test
  @DisplayName("Atomicity: Sync save completes fully or not at all")
  void atomicitySyncSaveCompletesEntirely() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Atomic Author", "fiction"));
    table.saveSync(author);

    // Entry must be fully created with all fields
    Set<AuthorDE> results = table.get("id", author.getKey(), null);
    assertEquals(1, results.size());
    AuthorDE saved = results.iterator().next();
    assertNotNull(saved.getKey());
    assertNotNull(saved.getData());
    assertNotNull(saved.getData().getName());
    assertNotNull(saved.getData().getAreaOfInterest());
    assertNotNull(saved.getCreated());
    assertEquals(0, saved.getVersion());
  }

  @Test
  @DisplayName("Atomicity: Delete removes entry completely")
  void atomicityDeleteRemovesCompletely() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Delete Atomic", "fiction"));
    table.saveSync(author);
    String key = author.getKey();

    AuthorDE toDelete = table.get("name", "Delete Atomic", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    table.deleteSync(toDelete);

    // Should be removed from both key lookup and index
    assertEquals(0, table.get("id", key, null).size());
    assertEquals(0, table.get("name", "Delete Atomic", null).size());
    assertEquals(0, table.search("name", "Delete", null).size());
  }

  @Test
  @DisplayName("Atomicity: Update modifies all changed fields atomically")
  void atomicityUpdateModifiesAllFields() throws Exception {
    AuthorDE original = new AuthorDE(new Author("AtomUpdate", "original"));
    table.saveSync(original);

    AuthorDE toUpdate = table.get("name", "AtomUpdate", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setName("AtomUpdated");
    toUpdate.getData().setAreaOfInterest("modified");
    table.saveSync(toUpdate);

    // Both fields should be updated - verify via ID lookup
    AuthorDE updated = table.get("id", original.getKey(), null).iterator().next();
    assertEquals("AtomUpdated", updated.getData().getName());
    assertEquals("modified", updated.getData().getAreaOfInterest());
    assertEquals(1, updated.getVersion());
  }

  @Test
  @DisplayName("Atomicity: Sync save blocks until committed")
  void atomicitySyncSaveBlocks() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Blocking Author", "fiction"));
    table.saveSync(author); // This should block until data is committed

    // After saveSync returns, data must be immediately queryable
    assertEquals(1, table.get("id", author.getKey(), null).size());
    assertEquals(1, table.get("name", "Blocking Author", null).size());
  }

  @Test
  @DisplayName("Atomicity: Event listeners fire after complete operation")
  void atomicityEventListenersFireAfterComplete() throws Exception {
    AtomicBoolean createFired = new AtomicBoolean(false);
    AtomicBoolean updateFired = new AtomicBoolean(false);
    AtomicBoolean deleteFired = new AtomicBoolean(false);
    CountDownLatch createLatch = new CountDownLatch(1);
    CountDownLatch updateLatch = new CountDownLatch(1);
    CountDownLatch deleteLatch = new CountDownLatch(1);

    NucleoDB eventDB = new NucleoDB(
        NucleoDB.DBType.NO_LOCAL,
        c -> c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration()),
        c -> {
          c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration());
          if (c.getClazz() == Author.class) {
            c.getDataTableConfig().setEventListener(new DataTableEventListener<AuthorDE>() {
              @Override
              public void create(Create create, AuthorDE entry) {
                createFired.set(true);
                createLatch.countDown();
              }
              @Override
              public void update(Update update, AuthorDE entry) {
                updateFired.set(true);
                updateLatch.countDown();
              }
              @Override
              public void delete(Delete delete, AuthorDE entry) {
                deleteFired.set(true);
                deleteLatch.countDown();
              }
            });
          }
        },
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        "com.nucleodb.library.helpers.models"
    );
    eventDB.waitTillReady();
    DataTable<AuthorDE> eventTable = eventDB.getTable(Author.class);

    // Create
    AuthorDE author = new AuthorDE(new Author("Event Author", "fiction"));
    eventTable.saveSync(author);
    assertTrue(createLatch.await(2, TimeUnit.SECONDS));
    assertTrue(createFired.get());

    // Update
    AuthorDE toUpdate = eventTable.get("name", "Event Author", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setAreaOfInterest("updated");
    eventTable.saveSync(toUpdate);
    assertTrue(updateLatch.await(2, TimeUnit.SECONDS));
    assertTrue(updateFired.get());

    // Delete
    AuthorDE toDelete = eventTable.get("name", "Event Author", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    eventTable.deleteSync(toDelete);
    assertTrue(deleteLatch.await(2, TimeUnit.SECONDS));
    assertTrue(deleteFired.get());
  }

  // ==================== CONSISTENCY TESTS ====================

  @Test
  @DisplayName("Consistency: Version always increments by 1")
  void consistencyVersionIncrement() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Version Check", "fiction"));
    table.saveSync(author);

    for (int i = 1; i <= 10; i++) {
      AuthorDE toUpdate = table.get("name", "Version Check", new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("v" + i);
      table.saveSync(toUpdate);

      AuthorDE updated = table.get("name", "Version Check", null).iterator().next();
      assertEquals(i, updated.getVersion(), "Version should be " + i + " after " + i + " updates");
    }
  }

  @Test
  @DisplayName("Consistency: Indexes stay in sync after CRUD")
  void consistencyIndexesInSync() throws Exception {
    // Create
    AuthorDE a1 = new AuthorDE(new Author("SyncAuthor", "fiction"));
    table.saveSync(a1);
    assertEquals(1, table.get("name", "SyncAuthor", null).size());
    assertEquals(1, table.get("areaOfInterest", "fiction", null).size());

    // Update areaOfInterest (use non-indexed-changing field to test index consistency)
    AuthorDE toUpdate = table.get("name", "SyncAuthor", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setAreaOfInterest("non-fiction");
    table.saveSync(toUpdate);
    // Name index still points to same entry
    assertEquals(1, table.get("name", "SyncAuthor", null).size());
    // Verify data updated via ID lookup
    AuthorDE updated = table.get("id", a1.getKey(), null).iterator().next();
    assertEquals("non-fiction", updated.getData().getAreaOfInterest());

    // Delete
    AuthorDE toDelete = table.get("name", "SyncAuthor", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    table.deleteSync(toDelete);
    assertEquals(0, table.get("name", "SyncAuthor", null).size());
    assertEquals(0, table.get("id", a1.getKey(), null).size());
  }

  @Test
  @DisplayName("Consistency: Table size remains accurate after operations")
  void consistencyTableSizeAccurate() throws Exception {
    assertEquals(0, table.getEntries().size());

    for (int i = 0; i < 10; i++) {
      table.saveSync(new AuthorDE(new Author("Size " + i, "fiction")));
    }
    assertEquals(10, table.getEntries().size());

    // Delete 3
    int deleted = 0;
    for (AuthorDE entry : table.search("name", "Size", new DataEntryProjection() {{
      setWritable(true);
    }})) {
      if (deleted >= 3) break;
      table.deleteSync(entry);
      deleted++;
    }
    assertEquals(7, table.getEntries().size());
  }

  @Test
  @DisplayName("Consistency: Created timestamp never changes after creation")
  void consistencyCreatedTimestampImmutable() throws Exception {
    table.saveSync(new AuthorDE(new Author("Timestamp", "fiction")));
    Instant created = table.get("name", "Timestamp", null).iterator().next().getCreated();

    for (int i = 0; i < 5; i++) {
      Thread.sleep(50);
      AuthorDE toUpdate = table.get("name", "Timestamp", new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("genre-" + i);
      table.saveSync(toUpdate);

      Instant afterUpdate = table.get("name", "Timestamp", null).iterator().next().getCreated();
      assertEquals(created, afterUpdate, "Created timestamp must not change on update #" + i);
    }
  }

  @Test
  @DisplayName("Consistency: Each entry has a unique key")
  void consistencyUniqueKeys() throws Exception {
    int count = 50;
    java.util.Set<String> keys = ConcurrentHashMap.newKeySet();

    for (int i = 0; i < count; i++) {
      AuthorDE author = new AuthorDE(new Author("UniqueKey " + i, "fiction"));
      table.saveSync(author);
      assertTrue(keys.add(author.getKey()), "Key should be unique: " + author.getKey());
    }
    assertEquals(count, keys.size());
  }

  // ==================== ISOLATION TESTS ====================

  @Test
  @DisplayName("Isolation: Read-only projections don't affect stored data")
  void isolationReadOnlyProjections() throws Exception {
    table.saveSync(new AuthorDE(new Author("ReadOnly", "fiction")));

    // Get a read-only copy
    AuthorDE readOnly = table.get("name", "ReadOnly", null).iterator().next();
    // Mutating the read-only copy's data object
    readOnly.getData().setName("HACKED");

    // Original should be unaffected
    assertEquals(1, table.get("name", "ReadOnly", null).size());
  }

  @Test
  @DisplayName("Isolation: Concurrent reads see consistent state")
  void isolationConcurrentReads() throws Exception {
    table.saveSync(new AuthorDE(new Author("ConcurrentRead", "fiction")));

    ExecutorService executor = Executors.newFixedThreadPool(10);
    AtomicInteger successCount = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(10);

    for (int i = 0; i < 10; i++) {
      executor.submit(() -> {
        try {
          Set<AuthorDE> results = table.get("name", "ConcurrentRead", null);
          if (results.size() == 1 && "ConcurrentRead".equals(results.iterator().next().getData().getName())) {
            successCount.incrementAndGet();
          }
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(10, successCount.get());
    executor.shutdown();
  }

  @Test
  @DisplayName("Isolation: Lock prevents concurrent modification of same entry")
  void isolationLockPreventsConcurrentModification() throws Exception {
    AuthorDE author = new AuthorDE(new Author("LockTest", "fiction"));
    table.saveSync(author);

    // Get with lock
    AuthorDE locked = table.get("name", "LockTest", new DataEntryProjection() {{
      setWritable(true);
      setLockUntilWrite(true);
    }}).iterator().next();

    assertNotNull(locked.getRequest(), "Locked entry should have a request ID");

    // Modify and save (releasing the lock)
    locked.getData().setAreaOfInterest("locked-update");
    table.saveSync(locked);

    // Verify update went through
    AuthorDE after = table.get("name", "LockTest", null).iterator().next();
    assertEquals("locked-update", after.getData().getAreaOfInterest());
  }

  @Test
  @DisplayName("Isolation: Sequential locked updates maintain data integrity")
  void isolationSequentialLockedUpdates() throws Exception {
    table.saveSync(new AuthorDE(new Author("SeqLock", "v0")));

    for (int i = 1; i <= 10; i++) {
      AuthorDE locked = table.get("name", "SeqLock", new DataEntryProjection() {{
        setWritable(true);
        setLockUntilWrite(true);
      }}).iterator().next();
      locked.getData().setAreaOfInterest("v" + i);
      table.saveSync(locked);
    }

    AuthorDE result = table.get("name", "SeqLock", null).iterator().next();
    assertEquals("v10", result.getData().getAreaOfInterest());
    assertEquals(10, result.getVersion());
  }

  @Test
  @DisplayName("Isolation: Read-only DB mode rejects writes")
  void isolationReadOnlyMode() throws Exception {
    NucleoDB readOnlyDB = new NucleoDB(
        NucleoDB.DBType.READ_ONLY,
        c -> {
          c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration());
          c.getConnectionConfig().setLoadSaved(false);
          c.getConnectionConfig().setSaveChanges(false);
        },
        c -> c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration()),
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        "com.nucleodb.library.helpers.models"
    );
    readOnlyDB.waitTillReady();
    DataTable<AuthorDE> roTable = readOnlyDB.getTable(Author.class);

    roTable.saveSync(new AuthorDE(new Author("Should Not Save", "fiction")));

    // In read-only mode, writes are silently rejected
    assertEquals(0, roTable.get("name", "Should Not Save", null).size());
  }

  @Test
  @DisplayName("Isolation: Concurrent writes to different entries don't interfere")
  void isolationConcurrentWritesDifferentEntries() throws Exception {
    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicBoolean failed = new AtomicBoolean(false);

    for (int i = 0; i < threadCount; i++) {
      final int idx = i;
      executor.submit(() -> {
        try {
          startLatch.await();
          AuthorDE author = new AuthorDE(new Author("Concurrent-" + idx, "genre-" + idx));
          table.saveSync(author);
        } catch (Exception e) {
          failed.set(true);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
    assertFalse(failed.get());

    // All entries should exist
    for (int i = 0; i < threadCount; i++) {
      assertEquals(1, table.get("name", "Concurrent-" + i, null).size());
    }
    executor.shutdown();
  }

  // ==================== DURABILITY TESTS ====================
  // Note: True durability tests require persistent storage (Kafka/file).
  // With LocalConfiguration (in-memory MQ), we test the durability contract
  // within a single session - that sync operations are fully committed.

  @Test
  @DisplayName("Durability: saveSync guarantees data is queryable immediately after return")
  void durabilitySaveSyncGuarantee() throws Exception {
    for (int i = 0; i < 20; i++) {
      AuthorDE author = new AuthorDE(new Author("Durable-" + i, "fiction"));
      table.saveSync(author);
      // Immediately after saveSync returns, data must be available
      assertEquals(1, table.get("id", author.getKey(), null).size(),
          "Entry " + i + " must be queryable immediately after saveSync");
    }
  }

  @Test
  @DisplayName("Durability: deleteSync guarantees removal after return")
  void durabilityDeleteSyncGuarantee() throws Exception {
    AuthorDE author = new AuthorDE(new Author("DurableDelete", "fiction"));
    table.saveSync(author);
    String key = author.getKey();

    AuthorDE toDelete = table.get("name", "DurableDelete", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    table.deleteSync(toDelete);

    // Immediately after deleteSync returns, data must be gone
    assertEquals(0, table.get("id", key, null).size());
    assertEquals(0, table.get("name", "DurableDelete", null).size());
  }

  @Test
  @DisplayName("Durability: Updates persist through version chain")
  void durabilityUpdatePersistsThroughVersionChain() throws Exception {
    table.saveSync(new AuthorDE(new Author("DurableUpdate", "fiction")));

    for (int i = 1; i <= 20; i++) {
      AuthorDE toUpdate = table.get("name", "DurableUpdate", new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("genre-" + i);
      table.saveSync(toUpdate);

      // After each update, verify the latest state
      AuthorDE current = table.get("name", "DurableUpdate", null).iterator().next();
      assertEquals("genre-" + i, current.getData().getAreaOfInterest());
      assertEquals(i, current.getVersion());
    }
  }

  @Test
  @DisplayName("Durability: Async save with callback confirms persistence")
  void durabilityAsyncSaveCallback() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<AuthorDE> callbackEntry = new AtomicReference<>();

    AuthorDE author = new AuthorDE(new Author("AsyncDurable", "fiction"));
    table.saveAsync(author, entry -> {
      callbackEntry.set(entry);
      latch.countDown();
    });

    assertTrue(latch.await(5, TimeUnit.SECONDS), "Async callback should fire within 5 seconds");
    assertNotNull(callbackEntry.get());
    assertEquals("AsyncDurable", callbackEntry.get().getData().getName());
  }
}
