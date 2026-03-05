package com.nucleodb.library.helpers.models;

import com.nucleodb.library.database.index.TrieIndex;
import com.nucleodb.library.database.index.annotation.Index;
import com.nucleodb.library.database.tables.annotation.Table;

import java.io.Serializable;

@Table(tableName = "book", dataEntryClass = BookDE.class)
public class Book implements Serializable {
  private static final long serialVersionUID = 1;
  @Index(type = TrieIndex.class)
  String title;

  @Index
  String genre;

  @Index
  int year;

  public Book() {
  }

  public Book(String title, String genre, int year) {
    this.title = title;
    this.genre = genre;
    this.year = year;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getGenre() {
    return genre;
  }

  public void setGenre(String genre) {
    this.genre = genre;
  }

  public int getYear() {
    return year;
  }

  public void setYear(int year) {
    this.year = year;
  }
}
