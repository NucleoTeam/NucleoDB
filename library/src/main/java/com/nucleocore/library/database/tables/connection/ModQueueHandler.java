package com.nucleocore.library.database.tables.connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.utils.Serializer;

import java.io.Serial;
import java.util.Queue;
import java.util.Stack;
import java.util.logging.Logger;

public class ModQueueHandler implements Runnable{
  private static Logger logger = Logger.getLogger(ModQueueHandler.class.getName());
  ConnectionHandler connectionHandler;

  public ModQueueHandler(ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
  }

  @Override
  public void run() {
    Queue<ModificationQueueItem> modqueue = connectionHandler.getModqueue();
    ObjectMapper om = new ObjectMapper().findAndRegisterModules();
    ModificationQueueItem mqi;
    int left = 0;
    boolean overkillCheck = false;
    while (true) {
      try{
        while (!modqueue.isEmpty() && (mqi = modqueue.poll())!=null) {
          connectionHandler.modify(mqi.getMod(), mqi.getModification());
          int leftTmp = connectionHandler.getLeftInModQueue().decrementAndGet();
          if(left == leftTmp){
            overkillCheck = true;
            break;
          }else{
            overkillCheck = false;
          }
          left = leftTmp;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      try {
        if(overkillCheck) {
          Thread.sleep(100);
          overkillCheck = false;
        } else {
          synchronized (modqueue) {
            if (connectionHandler.getLeftInModQueue().get() == 0) modqueue.wait();
            //logger.info(connectionHandler.getLeftInModQueue().get()+"");
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}