package com.nucleodb.library;

import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.*;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for asynchronous operations: saveAsync, deleteAsync, saveAndForget.
 */
class AsyncOperationsTest {

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

  // ==================== SAVE ASYNC ====================

  @Test
  @DisplayName("saveAsync creates entry and fires callback")
  void saveAsyncCreatesEntry() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<AuthorDE> callbackResult = new AtomicReference<>();

    AuthorDE author = new AuthorDE(new Author("Async Author", "fiction"));
    table.saveAsync(author, entry -> {
      callbackResult.set(entry);
      latch.countDown();
    });

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertNotNull(callbackResult.get());
    assertEquals("Async Author", callbackResult.get().getData().getName());
  }

  @Test
  @DisplayName("saveAsync multiple entries all complete")
  void saveAsyncMultiple() throws Exception {
    int count = 10;
    CountDownLatch latch = new CountDownLatch(count);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int i = 0; i < count; i++) {
      AuthorDE author = new AuthorDE(new Author("AsyncMulti-" + i, "fiction"));
      table.saveAsync(author, entry -> {
        if (entry != null) successCount.incrementAndGet();
        latch.countDown();
      });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    assertEquals(count, successCount.get());
    assertEquals(count, table.getEntries().size());
  }

  @Test
  @DisplayName("saveAsync update fires callback with updated entry")
  void saveAsyncUpdate() throws Exception {
    AuthorDE author = new AuthorDE(new Author("AsyncUpdate", "fiction"));
    table.saveSync(author);

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<AuthorDE> callbackResult = new AtomicReference<>();

    AuthorDE toUpdate = table.get("name", "AsyncUpdate", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setAreaOfInterest("updated");
    table.saveAsync(toUpdate, entry -> {
      callbackResult.set(entry);
      latch.countDown();
    });

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertNotNull(callbackResult.get());

    AuthorDE verified = table.get("id", author.getKey(), null).iterator().next();
    assertEquals("updated", verified.getData().getAreaOfInterest());
  }

  // ==================== DELETE ASYNC ====================

  @Test
  @DisplayName("deleteAsync removes entry and fires callback")
  void deleteAsyncRemovesEntry() throws Exception {
    AuthorDE author = new AuthorDE(new Author("AsyncDelete", "fiction"));
    table.saveSync(author);
    String key = author.getKey();

    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean callbackFired = new AtomicBoolean(false);

    AuthorDE toDelete = table.get("name", "AsyncDelete", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    table.deleteAsync(toDelete, entry -> {
      callbackFired.set(true);
      latch.countDown();
    });

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertTrue(callbackFired.get());

    // Wait a bit for deletion to propagate
    Thread.sleep(100);
    assertEquals(0, table.get("id", key, null).size());
  }

  @Test
  @DisplayName("deleteAsync multiple entries all complete")
  void deleteAsyncMultiple() throws Exception {
    for (int i = 0; i < 5; i++) {
      table.saveSync(new AuthorDE(new Author("AsyncDelMulti-" + i, "fiction")));
    }
    assertEquals(5, table.getEntries().size());

    CountDownLatch latch = new CountDownLatch(5);
    Set<AuthorDE> toDeleteSet = table.search("name", "AsyncDelMulti-", new DataEntryProjection() {{
      setWritable(true);
    }});
    for (AuthorDE entry : toDeleteSet) {
      table.deleteAsync(entry, e -> latch.countDown());
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    Thread.sleep(100);
    assertEquals(0, table.getEntries().size());
  }

  // ==================== SAVE AND FORGET ====================

  @Test
  @DisplayName("saveAndForget creates entry without callback")
  void saveAndForgetCreates() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Forgotten", "fiction"));
    boolean result = table.saveAndForget(author);
    assertTrue(result);

    // Wait for async processing
    Thread.sleep(500);
    assertEquals(1, table.get("id", author.getKey(), null).size());
  }

  @Test
  @DisplayName("saveAndForget multiple entries all persist")
  void saveAndForgetMultiple() throws Exception {
    for (int i = 0; i < 10; i++) {
      table.saveAndForget(new AuthorDE(new Author("Forget-" + i, "fiction")));
    }

    // Wait for all async processing
    Thread.sleep(2000);
    assertEquals(10, table.getEntries().size());
  }

  // ==================== MIXED SYNC/ASYNC ====================

  @Test
  @DisplayName("Mixed sync and async operations maintain consistency")
  void mixedSyncAsync() throws Exception {
    // Sync create
    AuthorDE syncAuthor = new AuthorDE(new Author("SyncFirst", "fiction"));
    table.saveSync(syncAuthor);

    // Async create
    CountDownLatch asyncLatch = new CountDownLatch(1);
    AuthorDE asyncAuthor = new AuthorDE(new Author("AsyncSecond", "poetry"));
    table.saveAsync(asyncAuthor, entry -> asyncLatch.countDown());
    assertTrue(asyncLatch.await(5, TimeUnit.SECONDS));

    // Both should exist
    assertEquals(1, table.get("name", "SyncFirst", null).size());
    assertEquals(1, table.get("name", "AsyncSecond", null).size());
    assertEquals(2, table.getEntries().size());

    // Sync delete one, async delete other
    AuthorDE toSyncDelete = table.get("name", "SyncFirst", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    table.deleteSync(toSyncDelete);

    CountDownLatch deleteLatch = new CountDownLatch(1);
    AuthorDE toAsyncDelete = table.get("name", "AsyncSecond", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    table.deleteAsync(toAsyncDelete, entry -> deleteLatch.countDown());
    assertTrue(deleteLatch.await(5, TimeUnit.SECONDS));

    Thread.sleep(100);
    assertEquals(0, table.getEntries().size());
  }

  // ==================== ASYNC ORDERING ====================

  @Test
  @DisplayName("Async operations complete in order per entry")
  void asyncOperationsOrderPerEntry() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Ordered", "v0"));
    table.saveSync(author);

    // Rapid sequential async updates
    CountDownLatch latch = new CountDownLatch(5);
    for (int i = 1; i <= 5; i++) {
      AuthorDE toUpdate = table.get("name", "Ordered", new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("v" + i);
      table.saveAsync(toUpdate, entry -> latch.countDown());
      // Wait for each to complete before next to ensure ordering
      Thread.sleep(100);
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    AuthorDE result = table.get("name", "Ordered", null).iterator().next();
    assertEquals("v5", result.getData().getAreaOfInterest());
  }
}
