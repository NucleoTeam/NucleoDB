package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.database.utils.ObjectFileWriter;

public class SaveHandler implements Runnable{
    ConnectionHandler connectionHandler;

    public SaveHandler(ConnectionHandler connectionHandler) {
      this.connectionHandler = connectionHandler;
    }

    @Override
    public void run() {
      long changedSaved = this.connectionHandler.getChanged();

      while (true) {
        try {
          if (this.connectionHandler.getChanged() > changedSaved) {
            //System.out.println("Saved connections");
            new ObjectFileWriter().writeObjectToFile(this.connectionHandler, "./data/"+connectionHandler.getConfig().getTopic()+".dat");
            changedSaved = this.connectionHandler.getChanged();
          }
          Thread.sleep(5000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

  }