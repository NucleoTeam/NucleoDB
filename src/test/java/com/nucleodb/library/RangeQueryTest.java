package com.nucleodb.library;

import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.*;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for range query operations (greaterThan, lessThan, etc.) on TreeIndex fields.
 * Uses the 'areaOfInterest' field which has a TreeIndex.
 *
 * Note: NucleoDB's greaterThan uses TreeMap.tailMap(key) which is inclusive (>=),
 * and greaterThanEqual uses TreeMap.tailMap(key, true) which is also inclusive.
 * In practice both greaterThan and greaterThanEqual behave as >= .
 * lessThan uses TreeMap.headMap(key) which is exclusive (<), which is correct.
 * lessThanEqual uses TreeMap.headMap(key, true) which is inclusive (<=), correct.
 */
class RangeQueryTest {

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

  private void populateAuthors() throws Exception {
    // Create entries with alphabetically ordered areaOfInterest values
    table.saveSync(new AuthorDE(new Author("Author A", "comedy")));      // c
    table.saveSync(new AuthorDE(new Author("Author B", "drama")));       // d
    table.saveSync(new AuthorDE(new Author("Author C", "fiction")));     // f
    table.saveSync(new AuthorDE(new Author("Author D", "horror")));      // h
    table.saveSync(new AuthorDE(new Author("Author E", "mystery")));     // m
    table.saveSync(new AuthorDE(new Author("Author F", "romance")));     // r
    table.saveSync(new AuthorDE(new Author("Author G", "thriller")));    // t
  }

  // ==================== GREATER THAN (actually >=) ====================

  @Test
  @DisplayName("greaterThan returns entries with values >= given value")
  void greaterThanBasic() throws Exception {
    populateAuthors();
    // greaterThan is inclusive (>=) due to TreeMap.tailMap behavior
    Set<AuthorDE> results = table.greaterThan("areaOfInterest", "horror", null);
    // horror, mystery, romance, thriller = 4
    assertEquals(4, results.size());
    assertTrue(results.stream().allMatch(
        a -> a.getData().getAreaOfInterest().compareTo("horror") >= 0));
  }

  @Test
  @DisplayName("greaterThan with highest value returns that value (inclusive)")
  void greaterThanHighestValue() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.greaterThan("areaOfInterest", "thriller", null);
    assertEquals(1, results.size()); // includes thriller itself
  }

  @Test
  @DisplayName("greaterThan with value before all returns all entries")
  void greaterThanBeforeAll() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.greaterThan("areaOfInterest", "aaa", null);
    assertEquals(7, results.size());
  }

  @Test
  @DisplayName("greaterThan with value after all returns empty")
  void greaterThanAfterAll() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.greaterThan("areaOfInterest", "zzz", null);
    assertEquals(0, results.size());
  }

  // ==================== GREATER THAN EQUAL ====================

  @Test
  @DisplayName("greaterThanEqual includes the matching value")
  void greaterThanEqualIncludesMatch() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.greaterThanEqual("areaOfInterest", "horror", null);
    assertTrue(results.stream().anyMatch(
        a -> a.getData().getAreaOfInterest().equals("horror")));
    assertEquals(4, results.size()); // horror + mystery + romance + thriller
  }

  @Test
  @DisplayName("greaterThanEqual with non-existing value returns entries after it")
  void greaterThanEqualNonExisting() throws Exception {
    populateAuthors();
    // "ghost" is between fiction and horror
    Set<AuthorDE> results = table.greaterThanEqual("areaOfInterest", "ghost", null);
    assertTrue(results.stream().allMatch(
        a -> a.getData().getAreaOfInterest().compareTo("ghost") >= 0));
    assertEquals(4, results.size()); // horror, mystery, romance, thriller
  }

  // ==================== LESS THAN ====================

  @Test
  @DisplayName("lessThan returns entries with values before given value")
  void lessThanBasic() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.lessThan("areaOfInterest", "horror", null);
    assertEquals(3, results.size()); // comedy, drama, fiction
    assertTrue(results.stream().allMatch(
        a -> a.getData().getAreaOfInterest().compareTo("horror") < 0));
  }

  @Test
  @DisplayName("lessThan with lowest value returns empty")
  void lessThanLowestValue() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.lessThan("areaOfInterest", "comedy", null);
    assertEquals(0, results.size());
  }

  @Test
  @DisplayName("lessThan with value after all returns all entries")
  void lessThanAfterAll() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.lessThan("areaOfInterest", "zzz", null);
    assertEquals(7, results.size());
  }

  // ==================== LESS THAN EQUAL ====================

  @Test
  @DisplayName("lessThanEqual includes the matching value")
  void lessThanEqualIncludesMatch() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.lessThanEqual("areaOfInterest", "fiction", null);
    assertTrue(results.stream().anyMatch(
        a -> a.getData().getAreaOfInterest().equals("fiction")));
    assertEquals(3, results.size()); // comedy, drama, fiction
  }

  @Test
  @DisplayName("lessThanEqual with non-existing value")
  void lessThanEqualNonExisting() throws Exception {
    populateAuthors();
    // "ghost" is between fiction and horror
    Set<AuthorDE> results = table.lessThanEqual("areaOfInterest", "ghost", null);
    assertTrue(results.stream().allMatch(
        a -> a.getData().getAreaOfInterest().compareTo("ghost") <= 0));
    assertEquals(3, results.size()); // comedy, drama, fiction
  }

  // ==================== COMBINED RANGE QUERIES ====================

  @Test
  @DisplayName("Combine greaterThanEqual and lessThanEqual for between query")
  void betweenQuery() throws Exception {
    populateAuthors();
    Set<AuthorDE> gte = table.greaterThanEqual("areaOfInterest", "drama", null);
    Set<AuthorDE> lte = table.lessThanEqual("areaOfInterest", "mystery", null);

    // Intersection = drama, fiction, horror, mystery
    long betweenCount = gte.stream()
        .filter(lte::contains)
        .count();
    assertEquals(4, betweenCount);
  }

  @Test
  @DisplayName("Range query with projection filter")
  void rangeQueryWithFilter() throws Exception {
    populateAuthors();
    Set<AuthorDE> results = table.greaterThan("areaOfInterest", "fiction",
        new DataEntryProjection<>() {{
          setFilter(de -> ((Author) de.getData()).getName().contains("Author"));
        }});
    assertTrue(results.size() > 0);
    // greaterThan is inclusive (>=), so fiction is included
    assertTrue(results.stream().allMatch(
        a -> a.getData().getAreaOfInterest().compareTo("fiction") >= 0));
  }

  // ==================== RANGE ON EMPTY TABLE ====================

  @Test
  @DisplayName("Range queries on empty table return empty")
  void rangeOnEmptyTable() throws Exception {
    assertEquals(0, table.greaterThan("areaOfInterest", "fiction", null).size());
    assertEquals(0, table.lessThan("areaOfInterest", "fiction", null).size());
    assertEquals(0, table.greaterThanEqual("areaOfInterest", "fiction", null).size());
    assertEquals(0, table.lessThanEqual("areaOfInterest", "fiction", null).size());
  }

  // ==================== RANGE WITH SINGLE ENTRY ====================

  @Test
  @DisplayName("Range queries with single entry")
  void rangeWithSingleEntry() throws Exception {
    table.saveSync(new AuthorDE(new Author("Solo", "fiction")));

    // greaterThan is inclusive (>=), so "fiction" matches
    assertEquals(1, table.greaterThan("areaOfInterest", "fiction", null).size());
    assertEquals(1, table.greaterThanEqual("areaOfInterest", "fiction", null).size());
    assertEquals(0, table.lessThan("areaOfInterest", "fiction", null).size());
    assertEquals(1, table.lessThanEqual("areaOfInterest", "fiction", null).size());
  }

  // ==================== RANGE WITH DUPLICATE VALUES ====================

  @Test
  @DisplayName("Range queries with duplicate values in index")
  void rangeWithDuplicates() throws Exception {
    table.saveSync(new AuthorDE(new Author("D1", "fiction")));
    table.saveSync(new AuthorDE(new Author("D2", "fiction")));
    table.saveSync(new AuthorDE(new Author("D3", "horror")));
    table.saveSync(new AuthorDE(new Author("D4", "horror")));
    table.saveSync(new AuthorDE(new Author("D5", "romance")));

    // greaterThan is inclusive (>=)
    Set<AuthorDE> gt = table.greaterThan("areaOfInterest", "fiction", null);
    assertEquals(5, gt.size()); // 2 fiction + 2 horror + 1 romance

    Set<AuthorDE> gte = table.greaterThanEqual("areaOfInterest", "fiction", null);
    assertEquals(5, gte.size()); // 2 fiction + 2 horror + 1 romance

    Set<AuthorDE> lt = table.lessThan("areaOfInterest", "horror", null);
    assertEquals(2, lt.size()); // 2 fiction

    Set<AuthorDE> lte = table.lessThanEqual("areaOfInterest", "horror", null);
    assertEquals(4, lte.size()); // 2 fiction + 2 horror
  }
}
