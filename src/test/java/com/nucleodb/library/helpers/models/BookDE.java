package com.nucleodb.library.helpers.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.table.DataEntry;

public class BookDE extends DataEntry<Book> {
  public BookDE(Book obj) {
    super(obj);
  }

  public BookDE(Create create) throws ClassNotFoundException, JsonProcessingException {
    super(create);
  }

  public BookDE() {
  }

  public BookDE(String key) {
    super(key);
  }
}
