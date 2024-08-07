package com.nucleodb.library;

import com.nucleodb.library.database.lock.LockConfig;
import com.nucleodb.library.database.lock.LockManager;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.annotation.Conn;
import com.nucleodb.library.database.tables.connection.ConnectionConfig;
import com.nucleodb.library.database.tables.connection.ConnectionConsumer;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.index.annotation.Index;
import com.nucleodb.library.database.tables.annotation.Table;
import com.nucleodb.library.database.tables.table.DataTableConfig;
import com.nucleodb.library.database.tables.table.DataTableConsumer;
import com.nucleodb.library.database.utils.TreeSetExt;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.database.utils.sql.SQLHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.tables.table.DataTableBuilder;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.utils.StartupRun;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.reflections.Reflections;
import java.beans.IntrospectionException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.nucleodb.library.utils.EnvReplace.replaceEnvVariables;
import static com.nucleodb.library.utils.field.FieldFinder.getAllAnnotatedFields;

public class NucleoDB{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());
  private TreeMap<String, DataTable> tables = new TreeMap<>();
  static String latestSave = "";

  private TreeMap<String, ConnectionHandler> connections = new TreeMap<>();

  private LockManager lockManager;

  public NucleoDB() {
  }

  public enum DBType{
    NO_LOCAL,
    READ_ONLY,
    EXPORT,
    ALL;
  }

  public NucleoDB(String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(DBType.ALL, null, null, null, packagesToScan);
  }

  public NucleoDB(Consumer<ConnectionConsumer> connectionCustomizer, Consumer<DataTableConsumer> dataTableCustomizer, Consumer<LockConfig> lockCustomizer, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(DBType.ALL, connectionCustomizer, dataTableCustomizer, lockCustomizer, packagesToScan);
  }
  public NucleoDB(DBType dbType, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(dbType, null, null, null, null, packagesToScan);
  }
  public NucleoDB(DBType dbType, Consumer<ConnectionConsumer> connectionCustomizer, Consumer<DataTableConsumer> dataTableCustomizer, Consumer<LockConfig> lockCustomizer, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this(dbType, null, connectionCustomizer, dataTableCustomizer, lockCustomizer, packagesToScan);
  }
  public NucleoDB(DBType dbType, String readToTime, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    startLockManager(null);
    startTables(packagesToScan, dbType, readToTime, null);
    startConnections(packagesToScan, dbType, readToTime, null);
  }
  public NucleoDB(DBType dbType, String readToTime, Consumer<ConnectionConsumer> connectionCustomizer, Consumer<DataTableConsumer> dataTableCustomizer, Consumer<LockConfig> lockCustomizer, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    startLockManager(lockCustomizer);
    startTables(packagesToScan, dbType, readToTime, dataTableCustomizer);

    startConnections(packagesToScan, dbType, readToTime, connectionCustomizer);
  }
  private void startLockManager(Consumer<LockConfig> customizer) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    LockConfig config = new LockConfig();
    CountDownLatch lockManagerStartupComplete = new CountDownLatch(1);
    config.setStartupRun(new StartupRun(){
      @Override
      public void run(LockManager lockManager) {
        lockManagerStartupComplete.countDown();
      }
    });
    if(customizer!=null) customizer.accept(config);
    lockManager = new LockManager(config);
    new Thread(lockManager).start();
    try {
      lockManagerStartupComplete.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  private void startConnections(String[] packagesToScan, DBType dbType, String readToTime, Consumer<ConnectionConsumer> customizer) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    logger.info("NucleoDB Connections Starting");
    Optional<Set<Class<?>>> connectionTypesOptional = Arrays.stream(packagesToScan).map(packageToScan->new Reflections(replaceEnvVariables(packageToScan)).getTypesAnnotatedWith(Conn.class)).reduce((a, b)->{
      a.addAll(b);
      return a;
    });
    if(!connectionTypesOptional.isPresent()){
      return;
    }
    Set<Class<?>> connectionTypes = connectionTypesOptional.get();
    CountDownLatch latch = new CountDownLatch(connectionTypes.size());
    for (Class<?> type : connectionTypes) {
      Conn connectionType = type.getAnnotation(Conn.class);
      String topic = String.format("%ss",connectionType.value().toLowerCase());
      ConnectionConfig config = new ConnectionConfig();
      config.setTopic(topic);

      Type[] actualTypeArguments = ((ParameterizedType) type.getGenericSuperclass()).getActualTypeArguments();
      Type[] toTableTypeArguments = new Type[0];
      Type[] fromTableTypeArguments = new Type[0];
      if(actualTypeArguments.length==2) {
        Class<?> toTable = (Class<?>) actualTypeArguments[1];
        toTableTypeArguments = ((ParameterizedType) toTable.getGenericSuperclass()).getActualTypeArguments();
        Class<?> fromTable = (Class<?>) actualTypeArguments[0];
        fromTableTypeArguments = ((ParameterizedType) fromTable.getGenericSuperclass()).getActualTypeArguments();
        
      }
      if(toTableTypeArguments.length!=1 && fromTableTypeArguments.length!=1) {
        System.exit(1);
      }
      config.setToTable((Class<?>) toTableTypeArguments[0]);
      logger.info("To Table " + config.getToTable().getName());
      config.setFromTable((Class<?>) fromTableTypeArguments[0]);
      logger.info("From table " + config.getFromTable().getName());
      config.setConnectionClass(type);
      if(readToTime!=null) {
        try {
          config.setReadToTime(Instant.parse(readToTime));
        }catch (DateTimeParseException e){
          e.printStackTrace();
        }
      }
      config.setLabel(connectionType.value().toUpperCase());
      if(customizer!=null) customizer.accept(new ConnectionConsumer(toTableTypeArguments[0], fromTableTypeArguments[0], config));
      config.setStartupRun(new StartupRun(){
        public void run(ConnectionHandler connectionHandler) {
          latch.countDown();
        }
      });
      switch (dbType) {
        case NO_LOCAL -> {
          config.setSaveChanges(false);
          config.setLoadSaved(false);
        }
        case READ_ONLY -> {
          config.setWrite(false);
        }
        case EXPORT -> config.setJsonExport(true);
      }
      connections.put(connectionType.value().toUpperCase(), new ConnectionHandler(this, config));
    }

    try {

      latch.await();
      logger.info("NucleoDB Connections Started");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  private void startTables(String[] packagesToScan, DBType dbType, String readToTime, Consumer<DataTableConsumer> customizer) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
    logger.info("NucleoDB Tables Starting");
    Optional<Set<Class<?>>> tableTypesOptional = Arrays.stream(packagesToScan).map(packageToScan->new Reflections(replaceEnvVariables(packageToScan)).getTypesAnnotatedWith(Table.class)).reduce((a, b)->{
      a.addAll(b);
      return a;
    });
    if(!tableTypesOptional.isPresent()){
      return;
    }

    Set<Class<?>> tableTypes = tableTypesOptional.get();
    CountDownLatch latch = new CountDownLatch(tableTypes.size());
    Set<DataTableBuilder> tables = new TreeSetExt<>();
    Map<String, Set<DataTableConfig.IndexConfig>> indexes = new TreeMap<>();
    for (Class<?> type : tableTypes) {
      Table tableAnnotation = type.getAnnotation(Table.class);
      String tableName = tableAnnotation.tableName();
      if(tableName.isEmpty()){
        tableName = type.getName().toLowerCase();
      }
      Class dataEntryClass = tableAnnotation.dataEntryClass();
      if(!DataEntry.class.isAssignableFrom(dataEntryClass)){
        throw new IncorrectDataEntryClassException(String.format("%s does not extend DataEntry", dataEntryClass.getName()));
      }
      try {
        if(dataEntryClass != DataEntry.class) {
          dataEntryClass.getDeclaredConstructor(type);
        }
        dataEntryClass.getDeclaredConstructor(Create.class);
        dataEntryClass.getDeclaredConstructor();
        dataEntryClass.getDeclaredConstructor(String.class);
      } catch (NoSuchMethodException e) {
        throw new MissingDataEntryConstructorsException(String.format("%s does not have all DataEntry constructors overridden!", dataEntryClass.getName()), e);
      }

      indexes.put(tableName, processIndexListForClass(type));

      switch (dbType) {
        case ALL -> tables.add(launchTable(tableName, dataEntryClass, type, new StartupRun(){
          public void run(DataTable table) {
            latch.countDown();
          }
        }, customizer));
        case NO_LOCAL -> tables.add(launchLocalOnlyTable(tableName, dataEntryClass, type, new StartupRun(){
          public void run(DataTable table) {
            latch.countDown();
          }
        }, customizer));
        case READ_ONLY -> tables.add(launchReadOnlyTable(tableName, dataEntryClass, type, new StartupRun(){
          public void run(DataTable table) {
            latch.countDown();
          }
        }, customizer));
        case EXPORT -> tables.add(launchExportOnlyTable(tableName, dataEntryClass, type, new StartupRun(){
          public void run(DataTable table) {
            latch.countDown();
          }
        }, customizer));
      }
    }
    tables.stream().forEach(table -> {
      if(readToTime!=null) {
        try {
          table.getConfig().setReadToTime(Instant.parse(readToTime));

        }catch (DateTimeParseException e){
          e.printStackTrace();
        }
      }
      table.addIndexes(indexes.get(table.getConfig().getTable()));
      try {
        DataTable build = table.build();
        build.setNucleoDB(this);
      } catch (IntrospectionException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    });
    try {

      latch.await();
      logger.info("NucleoDB Tables Started");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<DataTableConfig.IndexConfig> processIndexListForClass(Class<?> clazz) {
    Set<DataTableConfig.IndexConfig> indexes = new TreeSet<>();

    getAllAnnotatedFields(clazz, Index.class, "").forEach(field->{
      if (field.getAnnotation().value().isEmpty()) {
        indexes.add(new DataTableConfig.IndexConfig(field.getPath(), field.getAnnotation().type()) );
      } else {
        indexes.add(new DataTableConfig.IndexConfig(field.getAnnotation().value(), field.getAnnotation().type()));
      }
    });
    return indexes;
  }

  public <T> Object sql(String sqlStr) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);
      if (sqlStatement instanceof Select) {
        return SQLHandler.handleSelect((Select) sqlStatement, this, null);
      } else if (sqlStatement instanceof Insert) {
        return SQLHandler.handleInsert((Insert) sqlStatement, this);
      } else if (sqlStatement instanceof Update) {
        return SQLHandler.handleUpdate((Update) sqlStatement, this);
      } else if (sqlStatement instanceof Delete) {
        return SQLHandler.handleDelete((Delete) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public <T> Object sql(String sqlStr, Class clazz) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);

      if (sqlStatement instanceof Select) {
        return SQLHandler.handleSelect((Select) sqlStatement, this, clazz);
      } else if (sqlStatement instanceof Insert) {
        return SQLHandler.handleInsert((Insert) sqlStatement, this);
      } else if (sqlStatement instanceof Update) {
        return SQLHandler.handleUpdate((Update) sqlStatement, this);
      } else if (sqlStatement instanceof Delete) {
        return SQLHandler.handleDelete((Delete) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public <T> List<T> select(String sqlStr, Class clazz) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);

      if (sqlStatement instanceof Select) {
        return SQLHandler.handleSelect((Select) sqlStatement, this, clazz);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public DataEntry insert(String sqlStr) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);
      if (sqlStatement instanceof Insert) {
        return SQLHandler.handleInsert((Insert) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public boolean update(String sqlStr) {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);
      if (sqlStatement instanceof Update) {
        return SQLHandler.handleUpdate((Update) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public TreeMap<String, DataTable> getTables() {
    return tables;
  }

  public DataTable getTable(String table) {
    return tables.get(table);
  }

  public DataTable getTable(Class clazz) {
    Annotation annotation = clazz.getAnnotation(Table.class);
    if(annotation!=null) {
      return tables.get(((Table)annotation).tableName());
    }
    return null;
  }

  public DataTableBuilder launchTable(String table, Class dataEntryClass, Class clazz, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.create(table, clazz).setDataEntryClass(dataEntryClass).setDb(this);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchTable(String table, Class dataEntryClass, Class clazz, StartupRun runnable, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.create(table, clazz).setDataEntryClass(dataEntryClass).setDb(this).setStartupRun(runnable);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchReadOnlyTable(String table, Class dataEntryClass, Class clazz, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.createReadOnly(table, clazz).setDataEntryClass(dataEntryClass).setDb(this);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchReadOnlyTable(String table, Class dataEntryClass, Class clazz, StartupRun startupRun, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.createReadOnly(table, clazz).setDataEntryClass(dataEntryClass).setDb(this).setStartupRun(startupRun);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchLocalOnlyTable(String table, Class dataEntryClass, Class clazz, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.create(table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setSaveChanges(false).setDb(this);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchLocalOnlyTable(String table, Class dataEntryClass, Class clazz, StartupRun startupRun, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.create(table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setSaveChanges(false).setDb(this).setStartupRun(startupRun);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchExportOnlyTable(String table, Class dataEntryClass, Class clazz, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.create(table, clazz).setDataEntryClass(dataEntryClass).setJSONExport(true).setDb(this);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }
  public DataTableBuilder launchExportOnlyTable(String table, Class dataEntryClass, Class clazz, StartupRun startupRun, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.create(table, clazz).setDataEntryClass(dataEntryClass).setJSONExport(true).setDb(this).setStartupRun(startupRun);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchWriteOnlyTable(String table, Class dataEntryClass, Class clazz, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.createWriteOnly(table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setDb(this);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public DataTableBuilder launchWriteOnlyTable(String table, Class dataEntryClass, Class clazz, StartupRun startupRun, Consumer<DataTableConsumer> customizer) {
    DataTableBuilder dataTableBuilder = DataTableBuilder.createWriteOnly(table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setDb(this).setStartupRun(startupRun);
    if(customizer!=null) customizer.accept(new DataTableConsumer(clazz, dataTableBuilder.getConfig()));
    return dataTableBuilder;
  }

  public ConnectionHandler getConnectionHandler(Class clazz) {
    if(!clazz.isAnnotationPresent(Conn.class)){
      return null;
    }
    Conn conn = (Conn) clazz.getDeclaredAnnotation(Conn.class);
    return connections.get(conn.value().toUpperCase());
  }

  public TreeMap<String, ConnectionHandler> getConnections() {
    return connections;
  }

  public LockManager getLockManager() {

    return lockManager;
  }

  public void setLockManager(LockManager lockManager) {
    this.lockManager = lockManager;
  }
}
