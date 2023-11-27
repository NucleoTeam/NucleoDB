package com.nucleocore.library.examples.anime.connections;

import com.nucleocore.library.database.modifications.ConnectionCreate;
import com.nucleocore.library.database.tables.annotation.Conn;
import com.nucleocore.library.database.tables.connection.Connection;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.examples.anime.definitions.AnimeDE;
import com.nucleocore.library.examples.anime.tables.Anime;
import com.nucleocore.library.examples.anime.tables.User;

import java.util.Map;

@Conn(name = "WATCHING", to = Anime.class, from = User.class)
public class WatchingConnection extends Connection{
  public WatchingConnection() {
  }

  public WatchingConnection(DataEntry from, AnimeDE to) {
    super(from, to);
  }

  public WatchingConnection(ConnectionCreate connectionCreate) {
    super(connectionCreate);
  }

  public WatchingConnection(DataEntry from, AnimeDE to, Map<String, String> metadata) {
    super(from, to, metadata);
  }

  @Override
  public AnimeDE toEntry() {
    return (AnimeDE) super.toEntry();
  }
}
