package com.nucleodb.library;

import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Update;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.event.DataTableEventListener;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventTest{
  @Test
  public void createTest() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
    AtomicLong saved = new AtomicLong(0);
    AtomicLong deleted = new AtomicLong(0);
    AtomicLong updated = new AtomicLong(0);
    NucleoDB nucleoDB = new NucleoDB(
        NucleoDB.DBType.NO_LOCAL,
        c -> {
          c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration());
        },
        c -> {
          c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration());
          if(c.getClazz() == Author.class){
            c.getDataTableConfig().setEventListener(new DataTableEventListener<AuthorDE>(){
              @Override
              public void create(Create create, AuthorDE entry) {
                saved.incrementAndGet();
                synchronized (saved){
                  saved.notify();
                }
              }
              @Override
              public void update(Update update, AuthorDE entry) {
                updated.incrementAndGet();
                synchronized (updated){
                  updated.notify();
                }
              }

              @Override
              public void delete(Delete delete, AuthorDE entry) {
                deleted.incrementAndGet();
                synchronized (deleted){
                  deleted.notify();
                }
              }
            });
          }
        },
        c->{
          c.setMqsConfiguration(new LocalConfiguration());
        },
        "com.nucleodb.library.helpers.models"
    );
    nucleoDB.waitTillReady();
    try {
      DataTable<AuthorDE> table = nucleoDB.getTable(Author.class);
      AuthorDE authorDE = new AuthorDE(new Author("test", "testing"));
      table.saveSync(authorDE);
      synchronized (saved) {
        saved.wait(1000);
      }
      assertEquals(1, saved.get());
      AuthorDE first = table.get("name", "test", new DataEntryProjection(){{
        setWritable(true);
      }}).stream().findFirst().get();
      first.getData().setName("test2");
      table.saveSync(first);
      synchronized (updated) {
        updated.wait(1000);
      }
      assertEquals(1, updated.get());
      table.deleteSync(first);
      synchronized (deleted) {
        deleted.wait(1000);
      }
      assertEquals(1, deleted.get());

    } catch (InterruptedException e) {
      e.printStackTrace();

      throw new RuntimeException(e);
    } catch (IncorrectDataEntryObjectException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
