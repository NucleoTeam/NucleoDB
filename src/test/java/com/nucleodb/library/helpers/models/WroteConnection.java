package com.nucleodb.library.helpers.models;

import com.nucleodb.library.database.tables.annotation.Conn;
import com.nucleodb.library.database.tables.connection.Connection;

import java.util.Map;

@Conn("WROTE")
public class WroteConnection extends Connection<AuthorDE, BookDE> {
  public WroteConnection() {
  }

  public WroteConnection(AuthorDE from, BookDE to) {
    super(from, to);
  }

  public WroteConnection(AuthorDE from, BookDE to, Map<String, String> metadata) {
    super(from, to, metadata);
  }
}
