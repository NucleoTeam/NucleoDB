package com.nucleodb.library.models;

import com.nucleodb.library.database.index.TrieIndex;
import com.nucleodb.library.database.index.annotation.Index;
import com.nucleodb.library.database.tables.annotation.Table;

import java.io.Serializable;
import java.util.UUID;

@Table(tableName = "authorIT", dataEntryClass = AuthorDE.class)
public class Author implements Serializable {
  private static final long serialVersionUID = 1;
  @Index(type = TrieIndex.class)
  String name;

  @Index
  String areaOfInterest;

  public Author() {
  }

  public Author(String name, String areaOfInterest) {
    this.name = name;
    this.areaOfInterest = areaOfInterest;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAreaOfInterest() {
    return areaOfInterest;
  }

  public void setAreaOfInterest(String areaOfInterest) {
    this.areaOfInterest = areaOfInterest;
  }
}