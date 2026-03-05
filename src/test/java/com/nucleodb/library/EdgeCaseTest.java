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

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge case and boundary condition tests for NucleoDB operations.
 */
class EdgeCaseTest {

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

  // ==================== EMPTY STRING HANDLING ====================

  @Test
  @DisplayName("Create entry with empty string fields")
  void createWithEmptyStrings() throws Exception {
    AuthorDE author = new AuthorDE(new Author("", ""));
    authorTable.saveSync(author);

    AuthorDE saved = authorTable.get("id", author.getKey(), null).iterator().next();
    assertEquals("", saved.getData().getName());
    assertEquals("", saved.getData().getAreaOfInterest());
  }

  @Test
  @DisplayName("Search with empty string returns all entries via trie")
  void searchWithEmptyString() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("Alpha", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("Beta", "poetry")));

    Set<AuthorDE> results = authorTable.search("name", "", null);
    assertEquals(2, results.size());
  }

  // ==================== SPECIAL CHARACTERS ====================

  @Test
  @DisplayName("Create and retrieve entry with special characters")
  void specialCharactersInFields() throws Exception {
    String specialName = "O'Brien-Smith & Co. \"Test\" <tag>";
    AuthorDE author = new AuthorDE(new Author(specialName, "sci-fi/fantasy"));
    authorTable.saveSync(author);

    AuthorDE saved = authorTable.get("id", author.getKey(), null).iterator().next();
    assertEquals(specialName, saved.getData().getName());
  }

  @Test
  @DisplayName("Create and retrieve entry with unicode characters")
  void unicodeCharactersInFields() throws Exception {
    String unicodeName = "\u00e9\u00e0\u00fc\u00f1 \u4e16\u754c \ud55c\uad6d\uc5b4";
    AuthorDE author = new AuthorDE(new Author(unicodeName, "international"));
    authorTable.saveSync(author);

    AuthorDE saved = authorTable.get("id", author.getKey(), null).iterator().next();
    assertEquals(unicodeName, saved.getData().getName());
  }

  // ==================== NULL PROJECTION HANDLING ====================

  @Test
  @DisplayName("Query with null projection defaults gracefully")
  void queryWithNullProjection() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("NullProj", "fiction")));

    Set<AuthorDE> byKey = authorTable.get("name", "NullProj", null);
    assertEquals(1, byKey.size());

    Set<AuthorDE> bySearch = authorTable.search("name", "NullProj", null);
    assertEquals(1, bySearch.size());

    Set<AuthorDE> byIn = authorTable.in("areaOfInterest", List.of("fiction"), null);
    assertEquals(1, byIn.size());

    // getNotEqual only works when the comparison value exists in the index
    Set<AuthorDE> notEqual = authorTable.getNotEqual("areaOfInterest", "fiction", null);
    assertEquals(0, notEqual.size()); // only one entry with "fiction", no others
  }

  // ==================== EMPTY TABLE OPERATIONS ====================

  @Test
  @DisplayName("Query on empty table returns empty set")
  void queryOnEmptyTable() throws Exception {
    assertEquals(0, authorTable.get("name", "nobody", null).size());
    assertEquals(0, authorTable.search("name", "nobody", null).size());
    assertEquals(0, authorTable.getNotEqual("areaOfInterest", "fiction", null).size());
    assertEquals(0, authorTable.getEntries().size());
  }

  @Test
  @DisplayName("startsWith and endsWith on empty table")
  void startsWithEndsWithOnEmptyTable() throws Exception {
    assertEquals(0, authorTable.startsWith("name", "A", null).size());
    assertEquals(0, authorTable.endsWith("name", "z", null).size());
  }

  // ==================== PAGINATION EDGE CASES ====================

  @Test
  @DisplayName("Pagination skip beyond total returns empty")
  void paginationSkipBeyondTotal() throws Exception {
    for (int i = 0; i < 5; i++) {
      authorTable.saveSync(new AuthorDE(new Author("Page " + i, "fiction")));
    }

    Set<AuthorDE> results = authorTable.search("name", "Page",
        new DataEntryProjection(new Pagination(10, 5)));
    assertEquals(0, results.size());
  }

  @Test
  @DisplayName("Pagination with limit larger than result set")
  void paginationLimitLargerThanResults() throws Exception {
    for (int i = 0; i < 3; i++) {
      authorTable.saveSync(new AuthorDE(new Author("Limit " + i, "fiction")));
    }

    Set<AuthorDE> results = authorTable.search("name", "Limit",
        new DataEntryProjection(new Pagination(0, 100)));
    assertEquals(3, results.size());
  }

  @Test
  @DisplayName("Pagination with limit of 1 returns single entry")
  void paginationLimitOne() throws Exception {
    for (int i = 0; i < 5; i++) {
      authorTable.saveSync(new AuthorDE(new Author("Single " + i, "fiction")));
    }

    Set<AuthorDE> results = authorTable.search("name", "Single",
        new DataEntryProjection(new Pagination(0, 1)));
    assertEquals(1, results.size());
  }

  // ==================== IN QUERY EDGE CASES ====================

  @Test
  @DisplayName("In query with empty list returns empty set")
  void inQueryWithEmptyList() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("InTest", "fiction")));

    Set<AuthorDE> results = authorTable.in("areaOfInterest", List.of(), null);
    assertEquals(0, results.size());
  }

  @Test
  @DisplayName("In query with single value behaves like get")
  void inQueryWithSingleValue() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("SingleIn", "fiction")));

    Set<AuthorDE> inResult = authorTable.in("areaOfInterest", List.of("fiction"), null);
    Set<AuthorDE> getResult = authorTable.get("areaOfInterest", "fiction", null);
    assertEquals(getResult.size(), inResult.size());
  }

  @Test
  @DisplayName("In query with non-matching values returns empty")
  void inQueryWithNonMatchingValues() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("NoMatch", "fiction")));

    Set<AuthorDE> results = authorTable.in("areaOfInterest",
        List.of("fantasy", "romance", "horror"), null);
    assertEquals(0, results.size());
  }

  // ==================== NOT EQUAL EDGE CASES ====================

  @Test
  @DisplayName("getNotEqual excludes matching entries")
  void notEqualExcludesMatching() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("NE1", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("NE2", "poetry")));

    // getNotEqual returns entries that DON'T match the given value
    Set<AuthorDE> results = authorTable.getNotEqual("areaOfInterest", "fiction", null);
    assertEquals(1, results.size());
    assertEquals("poetry", results.iterator().next().getData().getAreaOfInterest());
  }

  @Test
  @DisplayName("getNotEqual returns empty when all match")
  void notEqualReturnsEmptyWhenAllMatch() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("AllSame1", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("AllSame2", "fiction")));

    Set<AuthorDE> results = authorTable.getNotEqual("areaOfInterest", "fiction", null);
    assertEquals(0, results.size());
  }

  @Test
  @DisplayName("getNotEqual with non-indexed value returns empty due to index limitation")
  void notEqualWithNonIndexedValue() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("NE3", "fiction")));
    authorTable.saveSync(new AuthorDE(new Author("NE4", "poetry")));

    // When the value doesn't exist in the index, getNotEqual returns empty set
    // This is a known limitation of the TreeIndex implementation
    Set<AuthorDE> results = authorTable.getNotEqual("areaOfInterest", "romance", null);
    assertEquals(0, results.size());
  }

  // ==================== STARTS WITH / ENDS WITH EDGE CASES ====================

  @Test
  @DisplayName("startsWith with full name matches exactly")
  void startsWithFullName() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("Complete Name", "fiction")));

    Set<AuthorDE> results = authorTable.startsWith("name", "Complete Name", null);
    assertEquals(1, results.size());
  }

  @Test
  @DisplayName("endsWith with full name matches exactly")
  void endsWithFullName() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("Full Match", "fiction")));

    Set<AuthorDE> results = authorTable.endsWith("name", "Full Match", null);
    assertEquals(1, results.size());
  }

  @Test
  @DisplayName("startsWith is case-sensitive")
  void startsWithCaseSensitive() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("CaseSensitive", "fiction")));

    assertEquals(1, authorTable.startsWith("name", "Case", null).size());
    assertEquals(0, authorTable.startsWith("name", "case", null).size());
  }

  // ==================== LARGE DATA ====================

  @Test
  @DisplayName("Create and query many entries")
  void createAndQueryManyEntries() throws Exception {
    int count = 100;
    for (int i = 0; i < count; i++) {
      authorTable.saveSync(new AuthorDE(new Author("Batch-" + i, "fiction")));
    }
    assertEquals(count, authorTable.getEntries().size());
    assertEquals(count, authorTable.search("name", "Batch-", null).size());
    assertEquals(count, authorTable.get("areaOfInterest", "fiction", null).size());
  }

  @Test
  @DisplayName("Long string values in fields")
  void longStringValues() throws Exception {
    String longName = "A".repeat(1000);
    AuthorDE author = new AuthorDE(new Author(longName, "fiction"));
    authorTable.saveSync(author);

    AuthorDE saved = authorTable.get("id", author.getKey(), null).iterator().next();
    assertEquals(longName, saved.getData().getName());
    assertEquals(1000, saved.getData().getName().length());
  }

  // ==================== MULTIPLE TABLE ISOLATION ====================

  @Test
  @DisplayName("Operations on one table don't affect another")
  void tableIsolation() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("Isolated Author", "fiction")));
    bookTable.saveSync(new BookDE(new Book("Isolated Book", "fiction", 2020)));

    assertEquals(1, authorTable.getEntries().size());
    assertEquals(1, bookTable.getEntries().size());

    // Delete from one table doesn't affect other
    AuthorDE toDelete = authorTable.get("name", "Isolated Author", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    authorTable.deleteSync(toDelete);

    assertEquals(0, authorTable.getEntries().size());
    assertEquals(1, bookTable.getEntries().size());
  }

  // ==================== RAPID SUCCESSIVE OPERATIONS ====================

  @Test
  @DisplayName("Rapid create-update-delete cycle")
  void rapidCreateUpdateDelete() throws Exception {
    for (int i = 0; i < 20; i++) {
      AuthorDE author = new AuthorDE(new Author("Rapid-" + i, "fiction"));
      authorTable.saveSync(author);

      AuthorDE toUpdate = authorTable.get("id", author.getKey(), new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      toUpdate.getData().setAreaOfInterest("updated-" + i);
      authorTable.saveSync(toUpdate);

      AuthorDE toDelete = authorTable.get("id", author.getKey(), new DataEntryProjection() {{
        setWritable(true);
      }}).iterator().next();
      authorTable.deleteSync(toDelete);
    }
    assertEquals(0, authorTable.getEntries().size());
  }

  // ==================== FILTER COMBINATIONS ====================

  @Test
  @DisplayName("Filter with pagination combines correctly")
  void filterWithPagination() throws Exception {
    for (int i = 0; i < 10; i++) {
      authorTable.saveSync(new AuthorDE(new Author("FilterPage " + i, i < 5 ? "fiction" : "poetry")));
    }

    Set<AuthorDE> filtered = authorTable.get("areaOfInterest", "fiction", new DataEntryProjection<>() {{
      setFilter(de -> ((Author) de.getData()).getName().contains("FilterPage"));
      setPagination(new Pagination(0, 3));
    }});
    assertEquals(3, filtered.size());
  }

  @Test
  @DisplayName("Filter that matches nothing returns empty set")
  void filterMatchesNothing() throws Exception {
    authorTable.saveSync(new AuthorDE(new Author("Exists", "fiction")));

    Set<AuthorDE> results = authorTable.get("areaOfInterest", "fiction", new DataEntryProjection<>() {{
      setFilter(de -> false);
    }});
    assertEquals(0, results.size());
  }

  @Test
  @DisplayName("Filter that matches all returns all")
  void filterMatchesAll() throws Exception {
    for (int i = 0; i < 5; i++) {
      authorTable.saveSync(new AuthorDE(new Author("All-" + i, "fiction")));
    }

    Set<AuthorDE> results = authorTable.get("areaOfInterest", "fiction", new DataEntryProjection<>() {{
      setFilter(de -> true);
    }});
    assertEquals(5, results.size());
  }
}
