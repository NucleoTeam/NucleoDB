package com.nucleodb.library;

import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Pagination;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.helpers.models.Book;
import com.nucleodb.library.helpers.models.BookDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CrudOperationsTest {

  NucleoDB nucleoDB;
  DataTable<AuthorDE> authorTable;
  DataTable<BookDE> bookTable;

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
  }

  // ==================== CREATE TESTS ====================

  @Test
  @Order(1)
  void createSingleEntryAndVerify() throws Exception {
    AuthorDE author = new AuthorDE(new Author("George Orwell", "science-fiction"));
    authorTable.saveSync(author);

    Set<AuthorDE> results = authorTable.get("id", author.getKey(), null);
    assertEquals(1, results.size());
    AuthorDE saved = results.iterator().next();
    assertEquals("George Orwell", saved.getData().getName());
    assertNotNull(saved.getKey());
    assertNotNull(saved.getCreated());
    assertEquals(0, saved.getVersion());
  }

  @Test
  @Order(2)
  void createMultipleEntriesWithUniqueKeys() throws Exception {
    AuthorDE a1 = new AuthorDE(new Author("Author A", "fiction"));
    AuthorDE a2 = new AuthorDE(new Author("Author B", "non-fiction"));
    AuthorDE a3 = new AuthorDE(new Author("Author C", "poetry"));
    authorTable.saveSync(a1);
    authorTable.saveSync(a2);
    authorTable.saveSync(a3);

    assertEquals(3, authorTable.getEntries().size());
    assertNotEquals(a1.getKey(), a2.getKey());
    assertNotEquals(a2.getKey(), a3.getKey());
  }

  @Test
  @Order(3)
  void createOnDifferentTables() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Author X", "fiction"));
    BookDE book = new BookDE(new Book("Book Y", "fiction", 2020));
    authorTable.saveSync(author);
    bookTable.saveSync(book);

    assertEquals(1, authorTable.getEntries().size());
    assertEquals(1, bookTable.getEntries().size());
  }

  // ==================== READ TESTS ====================

  @Test
  @Order(10)
  void readByPrimaryKeyAndIndex() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Jane Austen", "romance"));
    authorTable.saveSync(author);

    // By key
    assertEquals(1, authorTable.get("id", author.getKey(), null).size());
    // By indexed field
    assertEquals(1, authorTable.get("name", "Jane Austen", null).size());
    // Non-existent
    assertEquals(0, authorTable.get("id", "non-existent-key", null).size());
  }

  @Test
  @Order(11)
  void readByTrieSearch() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("George Orwell", "science-fiction")));
    authorTable.saveSync(new AuthorDE(new Author("George Martin", "fantasy")));
    authorTable.saveSync(new AuthorDE(new Author("Jane Austen", "romance")));

    Set<AuthorDE> results = authorTable.search("name", "George", null);
    assertEquals(2, results.size());
    assertTrue(results.stream().allMatch(a -> a.getData().getName().startsWith("George")));
  }

  @Test
  @Order(12)
  void readWithPaginationAndFilter() throws Exception {
    for (int i = 0; i < 10; i++) {
      authorTable.saveSync(new AuthorDE(new Author("Author " + i, "fiction")));
    }

    // Pagination
    Set<AuthorDE> page = authorTable.search("name", "Author", new DataEntryProjection(new Pagination(0, 5)));
    assertEquals(5, page.size());

    // Filter
    Set<AuthorDE> filtered = authorTable.get("areaOfInterest", "fiction", new DataEntryProjection<>() {{
      setFilter(de -> ((Author) de.getData()).getName().equals("Author 0"));
    }});
    assertEquals(1, filtered.size());
  }

  @Test
  @Order(13)
  void readWritableCopy() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Writable Test", "fiction"));
    authorTable.saveSync(author);

    AuthorDE writable = authorTable.get("name", "Writable Test", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    writable.getData().setName("Modified");
    authorTable.saveSync(writable);

    AuthorDE updated = authorTable.get("id", author.getKey(), null).iterator().next();
    assertEquals("Modified", updated.getData().getName());
  }

  @Test
  @Order(14)
  void readAllEntries() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("A1", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("A2", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("A3", "fiction")));

    assertEquals(3, authorTable.getEntries().size());
  }

  // ==================== UPDATE TESTS ====================

  @Test
  @Order(20)
  void updateFieldAndVerify() throws Exception {
    AuthorDE original = new AuthorDE(new Author("Update Me", "fiction"));
    authorTable.saveSync(original);

    AuthorDE toUpdate = authorTable.get("name", "Update Me", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setAreaOfInterest("non-fiction");
    authorTable.saveSync(toUpdate);

    AuthorDE updated = authorTable.get("id", original.getKey(), null).iterator().next();
    assertEquals("non-fiction", updated.getData().getAreaOfInterest());
    assertEquals("Update Me", updated.getData().getName());
  }

  @Test
  @Order(21)
  void updateIncrementsVersionAndModifiedTime() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Versioned", "fiction"));
    authorTable.saveSync(author);

    Instant created = authorTable.get("name", "Versioned", null).iterator().next().getCreated();
    Thread.sleep(50);

    AuthorDE toUpdate = authorTable.get("name", "Versioned", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setAreaOfInterest("updated-genre");
    authorTable.saveSync(toUpdate);

    AuthorDE v1 = authorTable.get("name", "Versioned", null).iterator().next();
    assertEquals(1, v1.getVersion());
    assertEquals(created, v1.getCreated());
    assertNotNull(v1.getModified());
    assertTrue(v1.getModified().isAfter(created));
  }

  @Test
  @Order(22)
  void updateMultipleFields() throws Exception {
    AuthorDE original = new AuthorDE(new Author("Multi Update", "fiction"));
    authorTable.saveSync(original);

    AuthorDE toUpdate = authorTable.get("name", "Multi Update", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setName("Multi Updated");
    toUpdate.getData().setAreaOfInterest("non-fiction");
    authorTable.saveSync(toUpdate);

    AuthorDE updated = authorTable.get("id", original.getKey(), null).iterator().next();
    assertEquals("Multi Updated", updated.getData().getName());
    assertEquals("non-fiction", updated.getData().getAreaOfInterest());
  }

  @Test
  @Order(23)
  void updateSequentialVersionIncrement() throws Exception {
    AuthorDE author = new AuthorDE(new Author("SeqVersion", "fiction"));
    authorTable.saveSync(author);

    for (int i = 1; i <= 5; i++) {
      AuthorDE toUpdate = authorTable.get("name", "SeqVersion", new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("genre-" + i);
      authorTable.saveSync(toUpdate);

      AuthorDE updated = authorTable.get("name", "SeqVersion", null).iterator().next();
      assertEquals(i, updated.getVersion());
    }
  }

  @Test
  @Order(24)
  void updateWithNoChangesDoesNotIncrementVersion() throws Exception {
    AuthorDE author = new AuthorDE(new Author("NoChange", "fiction"));
    authorTable.saveSync(author);

    AuthorDE toUpdate = authorTable.get("name", "NoChange", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    authorTable.saveSync(toUpdate);

    assertEquals(0, authorTable.get("name", "NoChange", null).iterator().next().getVersion());
  }

  // ==================== DELETE TESTS ====================

  @Test
  @Order(30)
  void deleteSingleEntry() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Delete Me", "fiction"));
    authorTable.saveSync(author);

    AuthorDE toDelete = authorTable.get("name", "Delete Me", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    authorTable.deleteSync(toDelete);

    assertEquals(0, authorTable.get("name", "Delete Me", null).size());
    assertEquals(0, authorTable.get("id", author.getKey(), null).size());
  }

  @Test
  @Order(31)
  void deleteRemovesFromIndexAndDecreasesCount() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("Indexed Delete", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("Indexed Keep", "fiction")));
    assertEquals(2, authorTable.getEntries().size());

    AuthorDE toDelete = authorTable.get("name", "Indexed Delete", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    authorTable.deleteSync(toDelete);

    assertEquals(0, authorTable.search("name", "Indexed Delete", null).size());
    assertEquals(1, authorTable.search("name", "Indexed Keep", null).size());
    assertEquals(1, authorTable.getEntries().size());
  }

  @Test
  @Order(32)
  void deleteAllEntries() throws Exception {
    for (int i = 0; i < 5; i++) {
      authorTable.saveSync(new AuthorDE(new Author("Batch " + i, "fiction")));
    }
    assertEquals(5, authorTable.getEntries().size());

    for (AuthorDE entry : authorTable.get("areaOfInterest", "fiction", new DataEntryProjection() {{
      setWritable(true);
    }})) {
      authorTable.deleteSync(entry);
    }
    assertEquals(0, authorTable.getEntries().size());
  }

  @Test
  @Order(33)
  void deleteAndRecreateSameData() throws Exception {
    AuthorDE author = new AuthorDE(new Author("Recreate", "fiction"));
    authorTable.saveSync(author);
    String originalKey = author.getKey();

    AuthorDE toDelete = authorTable.get("name", "Recreate", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    authorTable.deleteSync(toDelete);

    AuthorDE recreated = new AuthorDE(new Author("Recreate", "fiction"));
    authorTable.saveSync(recreated);

    assertEquals(1, authorTable.get("name", "Recreate", null).size());
    assertNotEquals(originalKey, recreated.getKey());
  }

  // ==================== SEARCH / INDEX TESTS ====================

  @Test
  @Order(40)
  void searchStartsWithAndEndsWith() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("George Orwell", "science-fiction")));
    authorTable.saveSync(new AuthorDE(new Author("George Martin", "fantasy")));
    authorTable.saveSync(new AuthorDE(new Author("Jane Austen", "romance")));

    assertEquals(2, authorTable.startsWith("name", "Geo", null).size());
    assertEquals(1, authorTable.endsWith("name", "ell", null).size());
  }

  @Test
  @Order(41)
  void searchWithTreeIndexAndNotEqual() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("A1", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("A2", "poetry")));
    authorTable.saveSync(new AuthorDE(new Author("A3", "fiction")));

    assertEquals(2, authorTable.get("areaOfInterest", "fiction", null).size());
    assertEquals(1, authorTable.getNotEqual("areaOfInterest", "fiction", null).size());
  }

  @Test
  @Order(42)
  void searchInMultipleValues() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("F1", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("P1", "poetry")));
    authorTable.saveSync(new AuthorDE(new Author("R1", "romance")));

    Set<AuthorDE> results = authorTable.in("areaOfInterest",
        java.util.List.of("fiction", "poetry"), null);
    assertEquals(2, results.size());
  }

  @Test
  @Order(43)
  void searchBooksByTitleAndGenre() throws Exception {
    bookTable.saveSync(new BookDE(new Book("The Great Gatsby", "fiction", 1925)));
    bookTable.saveSync(new BookDE(new Book("The Hobbit", "fantasy", 1937)));
    bookTable.saveSync(new BookDE(new Book("Animal Farm", "fiction", 1945)));

    // Trie search on title
    assertEquals(1, bookTable.search("title", "Animal", null).size());
    assertEquals(2, bookTable.startsWith("title", "The", null).size());
    // Tree index on genre
    assertEquals(2, bookTable.get("genre", "fiction", null).size());
  }
}
