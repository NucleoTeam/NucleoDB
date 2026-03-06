package com.nucleodb.library;

import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.connection.ConnectionProjection;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.helpers.models.*;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Connection (relationship) CRUD operations between entities.
 */
class ConnectionCrudTest {

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

  private AuthorDE createAuthor(String name, String genre) throws Exception {
    AuthorDE author = new AuthorDE(new Author(name, genre));
    authorTable.saveSync(author);
    return author;
  }

  private BookDE createBook(String title, String genre, int year) throws Exception {
    BookDE book = new BookDE(new Book(title, genre, year));
    bookTable.saveSync(book);
    return book;
  }

  // ==================== CREATE CONNECTION ====================

  @Test
  void createConnection() throws Exception {
    AuthorDE author = createAuthor("George Orwell", "fiction");
    BookDE book = createBook("1984", "dystopian", 1949);

    WroteConnection conn = new WroteConnection(author, book);
    wroteHandler.saveSync(conn);

    Set<WroteConnection> fromAuthor = wroteHandler.getByFrom(author, null);
    assertEquals(1, fromAuthor.size());
    assertEquals(book.getKey(), fromAuthor.iterator().next().getToKey());
  }

  @Test
  void createConnectionWithMetadata() throws Exception {
    AuthorDE author = createAuthor("Tolkien", "fantasy");
    BookDE book = createBook("The Hobbit", "fantasy", 1937);

    Map<String, String> metadata = new TreeMap<>();
    metadata.put("role", "primary-author");
    metadata.put("edition", "first");

    WroteConnection conn = new WroteConnection(author, book, metadata);
    wroteHandler.saveSync(conn);

    Set<WroteConnection> connections = wroteHandler.getByFrom(author, null);
    assertEquals(1, connections.size());
    WroteConnection saved = connections.iterator().next();
    assertEquals("primary-author", saved.getMetadata().get("role"));
    assertEquals("first", saved.getMetadata().get("edition"));
  }

  @Test
  void createMultipleConnectionsFromSameAuthor() throws Exception {
    AuthorDE author = createAuthor("Orwell", "fiction");
    BookDE book1 = createBook("1984", "dystopian", 1949);
    BookDE book2 = createBook("Animal Farm", "satire", 1945);

    wroteHandler.saveSync(new WroteConnection(author, book1));
    wroteHandler.saveSync(new WroteConnection(author, book2));

    Set<WroteConnection> connections = wroteHandler.getByFrom(author, null);
    assertEquals(2, connections.size());
  }

  @Test
  void createConnectionAssignsUUID() throws Exception {
    AuthorDE author = createAuthor("UUID Author", "fiction");
    BookDE book = createBook("UUID Book", "fiction", 2020);

    WroteConnection conn = new WroteConnection(author, book);
    wroteHandler.saveSync(conn);

    assertNotNull(conn.getUuid());
    Set<WroteConnection> all = wroteHandler.getAllConnections();
    assertEquals(1, all.size());
    assertNotNull(all.iterator().next().getUuid());
  }

  @Test
  void createConnectionSetsTimestamp() throws Exception {
    AuthorDE author = createAuthor("Time Author", "fiction");
    BookDE book = createBook("Time Book", "fiction", 2020);

    WroteConnection conn = new WroteConnection(author, book);
    wroteHandler.saveSync(conn);

    assertNotNull(conn.getDate());
    assertNotNull(conn.getModified());
  }

  // ==================== READ CONNECTION ====================

  @Test
  void readConnectionByFromKey() throws Exception {
    AuthorDE author = createAuthor("From Author", "fiction");
    BookDE book = createBook("From Book", "fiction", 2020);

    wroteHandler.saveSync(new WroteConnection(author, book));

    Set<WroteConnection> results = wroteHandler.getByFrom(author, null);
    assertEquals(1, results.size());
    assertEquals(author.getKey(), results.iterator().next().getFromKey());
  }

  @Test
  void readConnectionReverseByToKey() throws Exception {
    AuthorDE author = createAuthor("Reverse Author", "fiction");
    BookDE book = createBook("Reverse Book", "fiction", 2020);

    wroteHandler.saveSync(new WroteConnection(author, book));

    Set<WroteConnection> results = wroteHandler.getReverseByTo(book, null);
    assertEquals(1, results.size());
    assertEquals(author.getKey(), results.iterator().next().getFromKey());
  }

  @Test
  void readAllConnections() throws Exception {
    AuthorDE a1 = createAuthor("Auth1", "fiction");
    AuthorDE a2 = createAuthor("Auth2", "fiction");
    BookDE b1 = createBook("Book1", "fiction", 2020);
    BookDE b2 = createBook("Book2", "fiction", 2021);

    wroteHandler.saveSync(new WroteConnection(a1, b1));
    wroteHandler.saveSync(new WroteConnection(a1, b2));
    wroteHandler.saveSync(new WroteConnection(a2, b1));

    assertEquals(3, wroteHandler.getAllConnections().size());
  }

  @Test
  void readConnectionsByFromAndTo() throws Exception {
    AuthorDE author = createAuthor("Specific Author", "fiction");
    BookDE book1 = createBook("Specific Book 1", "fiction", 2020);
    BookDE book2 = createBook("Specific Book 2", "fiction", 2021);

    wroteHandler.saveSync(new WroteConnection(author, book1));
    wroteHandler.saveSync(new WroteConnection(author, book2));

    Set<WroteConnection> specific = wroteHandler.getByFromAndTo(author, book1, null);
    assertEquals(1, specific.size());
    assertEquals(book1.getKey(), specific.iterator().next().getToKey());
  }

  @Test
  void readNonExistentConnection() throws Exception {
    AuthorDE author = createAuthor("Lonely Author", "fiction");

    Set<WroteConnection> results = wroteHandler.getByFrom(author, null);
    assertEquals(0, results.size());
  }

  // ==================== UPDATE CONNECTION ====================

  @Test
  void updateConnectionMetadata() throws Exception {
    AuthorDE author = createAuthor("Update Author", "fiction");
    BookDE book = createBook("Update Book", "fiction", 2020);

    Map<String, String> metadata = new TreeMap<>();
    metadata.put("edition", "first");
    WroteConnection conn = new WroteConnection(author, book, metadata);
    wroteHandler.saveSync(conn);

    // Get writable copy and update
    WroteConnection toUpdate = wroteHandler.getByFrom(author, new ConnectionProjection<>() {{
      setWrite(true);
    }}).iterator().next();
    toUpdate.getMetadata().put("edition", "second");
    toUpdate.getMetadata().put("signed", "true");
    wroteHandler.saveSync(toUpdate);

    WroteConnection updated = wroteHandler.getByFrom(author, null).iterator().next();
    assertEquals("second", updated.getMetadata().get("edition"));
    assertEquals("true", updated.getMetadata().get("signed"));
  }

  @Test
  void updateConnectionIncrementsVersion() throws Exception {
    AuthorDE author = createAuthor("Version Author", "fiction");
    BookDE book = createBook("Version Book", "fiction", 2020);

    WroteConnection conn = new WroteConnection(author, book);
    wroteHandler.saveSync(conn);

    WroteConnection toUpdate = wroteHandler.getByFrom(author, new ConnectionProjection<>() {{
      setWrite(true);
    }}).iterator().next();
    toUpdate.getMetadata().put("key", "value");
    wroteHandler.saveSync(toUpdate);

    WroteConnection updated = wroteHandler.getByFrom(author, null).iterator().next();
    assertEquals(1, updated.getVersion());
  }

  // ==================== DELETE CONNECTION ====================

  @Test
  void deleteConnection() throws Exception {
    AuthorDE author = createAuthor("Del Author", "fiction");
    BookDE book = createBook("Del Book", "fiction", 2020);

    WroteConnection conn = new WroteConnection(author, book);
    wroteHandler.saveSync(conn);
    assertEquals(1, wroteHandler.getAllConnections().size());

    WroteConnection toDelete = wroteHandler.getByFrom(author, new ConnectionProjection<>() {{
      setWrite(true);
    }}).iterator().next();
    wroteHandler.deleteSync(toDelete);

    assertEquals(0, wroteHandler.getAllConnections().size());
    assertEquals(0, wroteHandler.getByFrom(author, null).size());
  }

  @Test
  void deleteConnectionRemovesFromReverseIndex() throws Exception {
    AuthorDE author = createAuthor("RevDel Author", "fiction");
    BookDE book = createBook("RevDel Book", "fiction", 2020);

    WroteConnection conn = new WroteConnection(author, book);
    wroteHandler.saveSync(conn);

    WroteConnection toDelete = wroteHandler.getByFrom(author, new ConnectionProjection<>() {{
      setWrite(true);
    }}).iterator().next();
    wroteHandler.deleteSync(toDelete);

    assertEquals(0, wroteHandler.getReverseByTo(book, null).size());
  }

  @Test
  void deleteOneConnectionKeepsOthers() throws Exception {
    AuthorDE author = createAuthor("KeepConn Author", "fiction");
    BookDE book1 = createBook("Keep Book 1", "fiction", 2020);
    BookDE book2 = createBook("Keep Book 2", "fiction", 2021);

    wroteHandler.saveSync(new WroteConnection(author, book1));
    wroteHandler.saveSync(new WroteConnection(author, book2));
    assertEquals(2, wroteHandler.getByFrom(author, null).size());

    WroteConnection toDelete = wroteHandler.getByFromAndTo(author, book1, new ConnectionProjection<>() {{
      setWrite(true);
    }}).iterator().next();
    wroteHandler.deleteSync(toDelete);

    assertEquals(1, wroteHandler.getByFrom(author, null).size());
    assertEquals(book2.getKey(), wroteHandler.getByFrom(author, null).iterator().next().getToKey());
  }

  // ==================== COMPLEX SCENARIOS ====================

  @Test
  void manyToManyRelationship() throws Exception {
    AuthorDE a1 = createAuthor("Author Alpha", "fiction");
    AuthorDE a2 = createAuthor("Author Beta", "fiction");
    BookDE b1 = createBook("Collab Book 1", "fiction", 2020);
    BookDE b2 = createBook("Collab Book 2", "fiction", 2021);

    // Both authors wrote both books
    wroteHandler.saveSync(new WroteConnection(a1, b1));
    wroteHandler.saveSync(new WroteConnection(a1, b2));
    wroteHandler.saveSync(new WroteConnection(a2, b1));
    wroteHandler.saveSync(new WroteConnection(a2, b2));

    assertEquals(2, wroteHandler.getByFrom(a1, null).size());
    assertEquals(2, wroteHandler.getByFrom(a2, null).size());
    assertEquals(2, wroteHandler.getReverseByTo(b1, null).size());
    assertEquals(2, wroteHandler.getReverseByTo(b2, null).size());
    assertEquals(4, wroteHandler.getAllConnections().size());
  }

  @Test
  void connectionSurvivesEntityUpdate() throws Exception {
    AuthorDE author = createAuthor("Stable Author", "fiction");
    BookDE book = createBook("Stable Book", "fiction", 2020);

    wroteHandler.saveSync(new WroteConnection(author, book));

    // Update the author
    AuthorDE toUpdate = authorTable.get("name", "Stable Author", new DataEntryProjection() {{
      setWritable(true);
    }}).iterator().next();
    toUpdate.getData().setAreaOfInterest("updated-genre");
    authorTable.saveSync(toUpdate);

    // Connection should still be retrievable
    Set<WroteConnection> connections = wroteHandler.getByFrom(author, null);
    assertEquals(1, connections.size());
  }
}
