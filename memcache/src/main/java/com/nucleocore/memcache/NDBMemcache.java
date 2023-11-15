package com.nucleocore.memcache;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.utils.StartupRun;

import java.io.*;
import java.net.*;

public class NDBMemcache{
  private final int port;

  public NDBMemcache(int port) {
    this.port = port;
  }

  public void start() {
    String bootstrap = System.getenv().getOrDefault("KAFKA_HOSTS", "127.0.0.1:29092");
    String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "memcache");
    new NucleoDB().launchTable(bootstrap, topic, KeyVal.class, new StartupRun(){
      @Override
      public void run(DataTable table) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
          System.out.println("NucleoDB MemCache server started on port " + port);
          while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(new ClientHandler(clientSocket, table)).start();
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }).setIndexes("name").build();
  }



  public static void main(String[] args) {
    int port = Integer.valueOf(System.getenv().getOrDefault("PORT", "11211")).intValue();
    NDBMemcache server = new NDBMemcache(port);
    server.start();
  }
}