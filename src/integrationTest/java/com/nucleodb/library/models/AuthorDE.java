package com.nucleodb.library.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.table.DataEntry;

public class AuthorDE extends DataEntry<Author>{
  public AuthorDE(Author obj) {
    super(obj);
  }

  public AuthorDE(Create create) throws ClassNotFoundException, JsonProcessingException {
    super(create);
  }

  public AuthorDE() {
  }

  public AuthorDE(String key) {
    super(key);
  }
}
