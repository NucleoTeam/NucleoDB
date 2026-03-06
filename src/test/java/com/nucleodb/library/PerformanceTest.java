package com.nucleodb.library;

import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.connection.ConnectionProjection;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.helpers.models.*;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance test suite measuring throughput (ops/sec) and latency (ms)
 * for NucleoDB CRUD operations.
 *
 * These tests are tagged with "performance" so they can be run separately
 * from the main test suite if desired.
 */
@Tag("performance")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PerformanceTest {

  private static final Logger logger = Logger.getLogger(PerformanceTest.class.getName());
  private static final int WARMUP_COUNT = 10;
  private static final int BENCHMARK_COUNT = 100;

  NucleoDB nucleoDB;
  DataTable<AuthorDE> authorTable;
  DataTable<BookDE> bookTable;
  ConnectionHandler<WroteConnection> wroteHandler;

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
    authorTable = nucleoDB.getTable(Author.class);
    bookTable = nucleoDB.getTable(Book.class);
    wroteHandler = nucleoDB.getConnectionHandler(WroteConnection.class);
  }

  @AfterEach
  public void teardown() throws Exception {
    for (WroteConnection conn : new java.util.ArrayList<>(wroteHandler.getAllConnections())) {
      wroteHandler.deleteSync(conn);
    }
    new java.util.ArrayList<>(authorTable.getEntries()).forEach(e -> {
      try { authorTable.deleteSync(e); } catch (InterruptedException ex) { throw new RuntimeException(ex); }
    });
    new java.util.ArrayList<>(bookTable.getEntries()).forEach(e -> {
      try { bookTable.deleteSync(e); } catch (InterruptedException ex) { throw new RuntimeException(ex); }
    });
  }

  private void logBenchmark(String operation, int count, long totalMs, List<Long> latencies) {
    latencies.sort(Long::compareTo);
    double throughput = (count * 1000.0) / totalMs;
    long min = latencies.get(0);
    long max = latencies.get(latencies.size() - 1);
    long median = latencies.get(latencies.size() / 2);
    long p95 = latencies.get((int)(latencies.size() * 0.95));
    long p99 = latencies.get((int)(latencies.size() * 0.99));
    double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0);

    logger.info(String.format(
        "\n===== %s BENCHMARK =====\n" +
        "  Operations:  %d\n" +
        "  Total time:  %d ms\n" +
        "  Throughput:  %.1f ops/sec\n" +
        "  Latency (ms):\n" +
        "    Min:    %d\n" +
        "    Avg:    %.2f\n" +
        "    Median: %d\n" +
        "    P95:    %d\n" +
        "    P99:    %d\n" +
        "    Max:    %d\n",
        operation, count, totalMs, throughput, min, avg, median, p95, p99, max
    ));
  }

  // ==================== CREATE THROUGHPUT & LATENCY ====================

  @Test
  @Order(1)
  void benchmarkSyncCreate() throws Exception {
    // Warmup
    for (int i = 0; i < WARMUP_COUNT; i++) {
      authorTable.saveSync(new AuthorDE(new Author("warmup-" + i, "warmup")));
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      long opStart = System.nanoTime();
      authorTable.saveSync(new AuthorDE(new Author("bench-create-" + i, "benchmark")));
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("SYNC CREATE", BENCHMARK_COUNT, totalMs, latencies);

    assertEquals(WARMUP_COUNT + BENCHMARK_COUNT, authorTable.getEntries().size());
    assertTrue(totalMs < 30000, "Create benchmark should complete within 30 seconds");
  }

  @Test
  @Order(2)
  void benchmarkAsyncCreate() throws Exception {
    CountDownLatch latch = new CountDownLatch(BENCHMARK_COUNT);
    AtomicInteger completed = new AtomicInteger(0);

    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      authorTable.saveAsync(new AuthorDE(new Author("async-create-" + i, "benchmark")), entry -> {
        completed.incrementAndGet();
        latch.countDown();
      });
    }

    assertTrue(latch.await(30, TimeUnit.SECONDS), "Async creates should complete within 30s");
    long totalMs = System.currentTimeMillis() - start;

    double throughput = (BENCHMARK_COUNT * 1000.0) / totalMs;
    logger.info(String.format(
        "\n===== ASYNC CREATE BENCHMARK =====\n" +
        "  Operations:  %d\n" +
        "  Total time:  %d ms\n" +
        "  Throughput:  %.1f ops/sec\n",
        BENCHMARK_COUNT, totalMs, throughput
    ));

    assertEquals(BENCHMARK_COUNT, completed.get());
  }

  // ==================== READ THROUGHPUT & LATENCY ====================

  @Test
  @Order(10)
  void benchmarkReadByKey() throws Exception {
    // Setup: create entries
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      AuthorDE author = new AuthorDE(new Author("read-key-" + i, "benchmark"));
      authorTable.saveSync(author);
      keys.add(author.getKey());
    }

    // Warmup
    for (int i = 0; i < WARMUP_COUNT; i++) {
      authorTable.get("id", keys.get(i), null);
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (String key : keys) {
      long opStart = System.nanoTime();
      Set<AuthorDE> result = authorTable.get("id", key, null);
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
      assertEquals(1, result.size());
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("READ BY KEY", BENCHMARK_COUNT, totalMs, latencies);
  }

  @Test
  @Order(11)
  void benchmarkReadByIndex() throws Exception {
    // Setup: create entries with unique names
    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      authorTable.saveSync(new AuthorDE(new Author("idx-author-" + i, "benchmark")));
    }

    // Warmup
    for (int i = 0; i < WARMUP_COUNT; i++) {
      authorTable.get("name", "idx-author-" + i, null);
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      long opStart = System.nanoTime();
      Set<AuthorDE> result = authorTable.get("name", "idx-author-" + i, null);
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
      assertEquals(1, result.size());
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("READ BY INDEX", BENCHMARK_COUNT, totalMs, latencies);
  }

  @Test
  @Order(12)
  void benchmarkTrieSearch() throws Exception {
    // Setup: create entries with predictable prefixes
    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      authorTable.saveSync(new AuthorDE(new Author("trie-search-" + i, "benchmark")));
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      long opStart = System.nanoTime();
      Set<AuthorDE> result = authorTable.search("name", "trie-search-" + i, null);
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
      assertTrue(result.size() >= 1);
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("TRIE SEARCH", BENCHMARK_COUNT, totalMs, latencies);
  }

  @Test
  @Order(13)
  void benchmarkReadWithPagination() throws Exception {
    // Setup: create many entries with same genre
    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      authorTable.saveSync(new AuthorDE(new Author("page-author-" + i, "paginated")));
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    int pageSize = 10;
    for (int page = 0; page < BENCHMARK_COUNT / pageSize; page++) {
      long opStart = System.nanoTime();
      Set<AuthorDE> result = authorTable.get("areaOfInterest", "paginated",
          new DataEntryProjection(new com.nucleodb.library.database.utils.Pagination(page * pageSize, pageSize)));
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
      assertEquals(pageSize, result.size());
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("READ WITH PAGINATION", BENCHMARK_COUNT / pageSize, totalMs, latencies);
  }

  // ==================== UPDATE THROUGHPUT & LATENCY ====================

  @Test
  @Order(20)
  void benchmarkSyncUpdate() throws Exception {
    // Setup: create an entry to update
    authorTable.saveSync(new AuthorDE(new Author("update-bench", "original")));

    // Warmup
    for (int i = 0; i < WARMUP_COUNT; i++) {
      AuthorDE toUpdate = authorTable.get("name", "update-bench", new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("warmup-" + i);
      authorTable.saveSync(toUpdate);
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      long opStart = System.nanoTime();
      AuthorDE toUpdate = authorTable.get("name", "update-bench", new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("bench-" + i);
      authorTable.saveSync(toUpdate);
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("SYNC UPDATE", BENCHMARK_COUNT, totalMs, latencies);

    AuthorDE result = authorTable.get("name", "update-bench", null).iterator().next();
    assertEquals(WARMUP_COUNT + BENCHMARK_COUNT, result.getVersion());
  }

  // ==================== DELETE THROUGHPUT & LATENCY ====================

  @Test
  @Order(30)
  void benchmarkSyncDelete() throws Exception {
    // Setup: create entries to delete
    List<AuthorDE> entries = new ArrayList<>();
    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      AuthorDE author = new AuthorDE(new Author("delete-bench-" + i, "deletable"));
      authorTable.saveSync(author);
      entries.add(author);
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      AuthorDE toDelete = authorTable.get("name", "delete-bench-" + i, new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();

      long opStart = System.nanoTime();
      authorTable.deleteSync(toDelete);
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("SYNC DELETE", BENCHMARK_COUNT, totalMs, latencies);

    assertEquals(0, authorTable.getEntries().size());
  }

  // ==================== CONNECTION THROUGHPUT & LATENCY ====================

  @Test
  @Order(40)
  void benchmarkConnectionCreate() throws Exception {
    // Setup: create authors and books
    AuthorDE author = new AuthorDE(new Author("conn-author", "fiction"));
    authorTable.saveSync(author);

    List<BookDE> books = new ArrayList<>();
    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      BookDE book = new BookDE(new Book("conn-book-" + i, "fiction", 2000 + i));
      bookTable.saveSync(book);
      books.add(book);
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      long opStart = System.nanoTime();
      wroteHandler.saveSync(new WroteConnection(author, books.get(i)));
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("CONNECTION CREATE", BENCHMARK_COUNT, totalMs, latencies);

    assertEquals(BENCHMARK_COUNT, wroteHandler.getByFrom(author, null).size());
  }

  @Test
  @Order(41)
  void benchmarkConnectionRead() throws Exception {
    // Setup: create author with many connections
    AuthorDE author = new AuthorDE(new Author("conn-read-author", "fiction"));
    authorTable.saveSync(author);

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      BookDE book = new BookDE(new Book("conn-read-book-" + i, "fiction", 2000 + i));
      bookTable.saveSync(book);
      wroteHandler.saveSync(new WroteConnection(author, book));
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      long opStart = System.nanoTime();
      Set<WroteConnection> result = wroteHandler.getByFrom(author, null);
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
      assertEquals(BENCHMARK_COUNT, result.size());
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("CONNECTION READ", BENCHMARK_COUNT, totalMs, latencies);
  }

  @Test
  @Order(42)
  void benchmarkConnectionDelete() throws Exception {
    // Setup
    AuthorDE author = new AuthorDE(new Author("conn-del-author", "fiction"));
    authorTable.saveSync(author);

    List<WroteConnection> connections = new ArrayList<>();
    for (int i = 0; i < BENCHMARK_COUNT; i++) {
      BookDE book = new BookDE(new Book("conn-del-book-" + i, "fiction", 2000 + i));
      bookTable.saveSync(book);
      WroteConnection conn = new WroteConnection(author, book);
      wroteHandler.saveSync(conn);
      connections.add(conn);
    }

    List<Long> latencies = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (WroteConnection conn : wroteHandler.getByFrom(author, new ConnectionProjection<>() {{
      setWrite(true);
    }})) {
      long opStart = System.nanoTime();
      wroteHandler.deleteSync(conn);
      latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
    }

    long totalMs = System.currentTimeMillis() - start;
    logBenchmark("CONNECTION DELETE", latencies.size(), totalMs, latencies);

    assertEquals(0, wroteHandler.getAllConnections().size());
  }

  // ==================== CONCURRENT THROUGHPUT ====================

  @Test
  @Order(50)
  void benchmarkConcurrentCreateThroughput() throws Exception {
    int threadCount = 4;
    int opsPerThread = BENCHMARK_COUNT / threadCount;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < opsPerThread; i++) {
            authorTable.saveSync(new AuthorDE(
                new Author("concurrent-" + threadId + "-" + i, "concurrent")));
            successCount.incrementAndGet();
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    long start = System.currentTimeMillis();
    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    long totalMs = System.currentTimeMillis() - start;

    double throughput = (successCount.get() * 1000.0) / totalMs;
    logger.info(String.format(
        "\n===== CONCURRENT CREATE BENCHMARK (%d threads) =====\n" +
        "  Operations:  %d\n" +
        "  Total time:  %d ms\n" +
        "  Throughput:  %.1f ops/sec\n",
        threadCount, successCount.get(), totalMs, throughput
    ));

    assertEquals(BENCHMARK_COUNT, successCount.get());
    assertEquals(BENCHMARK_COUNT, authorTable.getEntries().size());
    executor.shutdown();
  }

  @Test
  @Order(51)
  void benchmarkMixedWorkload() throws Exception {
    // Pre-populate with some data
    List<String> existingKeys = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      AuthorDE author = new AuthorDE(new Author("mixed-" + i, "mixed"));
      authorTable.saveSync(author);
      existingKeys.add(author.getKey());
    }

    List<Long> createLatencies = new ArrayList<>();
    List<Long> readLatencies = new ArrayList<>();
    List<Long> updateLatencies = new ArrayList<>();
    List<Long> deleteLatencies = new ArrayList<>();

    long start = System.currentTimeMillis();

    // Interleave operations
    for (int i = 0; i < 50; i++) {
      // CREATE
      long opStart = System.nanoTime();
      AuthorDE newAuthor = new AuthorDE(new Author("mixed-new-" + i, "mixed-new"));
      authorTable.saveSync(newAuthor);
      createLatencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));

      // READ
      opStart = System.nanoTime();
      authorTable.get("id", existingKeys.get(i), null);
      readLatencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));

      // UPDATE
      opStart = System.nanoTime();
      AuthorDE toUpdate = authorTable.get("name", "mixed-" + i, new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("updated-mixed");
      authorTable.saveSync(toUpdate);
      updateLatencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));

      // DELETE (the newly created one)
      opStart = System.nanoTime();
      AuthorDE toDelete = authorTable.get("id", newAuthor.getKey(), new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      authorTable.deleteSync(toDelete);
      deleteLatencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
    }

    long totalMs = System.currentTimeMillis() - start;

    logger.info(String.format(
        "\n===== MIXED WORKLOAD BENCHMARK =====\n" +
        "  Total time: %d ms\n" +
        "  50 creates, 50 reads, 50 updates, 50 deletes = 200 total ops\n" +
        "  Overall throughput: %.1f ops/sec\n",
        totalMs, (200 * 1000.0) / totalMs
    ));

    logBenchmark("MIXED - CREATE", 50, totalMs, createLatencies);
    logBenchmark("MIXED - READ", 50, totalMs, readLatencies);
    logBenchmark("MIXED - UPDATE", 50, totalMs, updateLatencies);
    logBenchmark("MIXED - DELETE", 50, totalMs, deleteLatencies);

    // Original 50 should still exist (since we deleted the new ones)
    assertEquals(50, authorTable.getEntries().size());
  }

  // ==================== SCALABILITY ====================

  @Test
  @Order(60)
  void benchmarkReadPerformanceWithGrowingDataset() throws Exception {
    int[] datasetSizes = {10, 50, 100, 200};

    for (int size : datasetSizes) {
      // Add entries to reach target size
      int currentSize = authorTable.getEntries().size();
      for (int i = currentSize; i < size; i++) {
        authorTable.saveSync(new AuthorDE(new Author("scale-" + i, "scalable")));
      }

      // Benchmark reads at this size
      List<Long> latencies = new ArrayList<>();
      long start = System.currentTimeMillis();

      for (int i = 0; i < 50; i++) {
        long opStart = System.nanoTime();
        authorTable.get("name", "scale-" + (i % size), null);
        latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStart));
      }

      long totalMs = System.currentTimeMillis() - start;
      logBenchmark("READ @ " + size + " entries", 50, totalMs, latencies);
    }
  }
}
