package com.nucleodb.library;

import com.google.common.collect.Queues;
import com.nucleodb.library.database.lock.LockConfig;
import com.nucleodb.library.database.lock.LockManager;
import com.nucleodb.library.database.tables.connection.ConnectionConsumer;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.tables.table.DataTableConsumer;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.utils.StartupRun;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.database.utils.sql.SQLHandler;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class NucleoDB {
    private static Logger logger = Logger.getLogger(NucleoDB.class.getName());
    static String latestSave = "";

    private final TableRegistry tableRegistry = new TableRegistry();
    private final ConnectionRegistry connectionRegistry = new ConnectionRegistry();
    private LockManager lockManager;
    private ShardConfig shardConfig;
    private Queue<CountDownLatch> latches = Queues.newArrayBlockingQueue(25);

    public enum DBType {
        NO_LOCAL,
        READ_ONLY,
        EXPORT,
        ALL;
    }

    public NucleoDB() {
    }

    public NucleoDB(Consumer<LockConfig> customizer) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        startLockManager(customizer);
    }

    public NucleoDB(String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(DBType.ALL, null, null, null, null, null, packagesToScan);
    }

    public NucleoDB(Consumer<ConnectionConsumer> connectionCustomizer, Consumer<DataTableConsumer> dataTableCustomizer, Consumer<LockConfig> lockCustomizer, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(DBType.ALL, null, connectionCustomizer, dataTableCustomizer, lockCustomizer, null, packagesToScan);
    }

    public NucleoDB(DBType dbType, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(dbType, null, null, null, null, null, packagesToScan);
    }

    public NucleoDB(DBType dbType, Consumer<ConnectionConsumer> connectionCustomizer, Consumer<DataTableConsumer> dataTableCustomizer, Consumer<LockConfig> lockCustomizer, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(dbType, null, connectionCustomizer, dataTableCustomizer, lockCustomizer, null, packagesToScan);
    }

    public NucleoDB(DBType dbType, String readToTime, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(dbType, readToTime, null, null, null, null, packagesToScan);
    }

    public NucleoDB(DBType dbType, String readToTime, Consumer<ConnectionConsumer> connectionCustomizer, Consumer<DataTableConsumer> dataTableCustomizer, Consumer<LockConfig> lockCustomizer, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(dbType, readToTime, connectionCustomizer, dataTableCustomizer, lockCustomizer, null, packagesToScan);
    }

    public NucleoDB(DBType dbType, String readToTime, Consumer<ConnectionConsumer> connectionCustomizer, Consumer<DataTableConsumer> dataTableCustomizer, Consumer<LockConfig> lockCustomizer, ShardConfig shardConfig, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this.shardConfig = shardConfig;
        startLockManager(lockCustomizer);
        if (packagesToScan != null) {
            CountDownLatch latch = tableRegistry.startTables(this, packagesToScan, dbType, readToTime, dataTableCustomizer, shardConfig);
            if (latch != null) {
                latches.add(latch);
            }
            latch = connectionRegistry.startConnections(this, packagesToScan, dbType, readToTime, connectionCustomizer, shardConfig);
            if (latch != null) {
                latches.add(latch);
            }
        }
    }

    public void startLockManager(Consumer<LockConfig> customizer) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        LockConfig config = new LockConfig();
        CountDownLatch lockManagerStartupComplete = new CountDownLatch(1);
        config.setStartupRun(new StartupRun() {
            @Override
            public void run(LockManager lockManager) {
                lockManagerStartupComplete.countDown();
            }
        });
        if (customizer != null) customizer.accept(config);
        lockManager = new LockManager(config);
        new Thread(lockManager).start();
        try {
            lockManagerStartupComplete.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // --- SQL methods ---

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

    // --- Delegate to TableRegistry ---

    public TreeMap<String, DataTable> getTables() {
        return tableRegistry.getTables();
    }

    public DataTable getTable(String table) {
        return tableRegistry.getTable(table);
    }

    public DataTable getTable(Class clazz) {
        return tableRegistry.getTable(clazz);
    }

    public void addTableEvent(Consumer<DataTable> dataTableConsumer) {
        tableRegistry.addTableEvent(dataTableConsumer);
    }

    // --- Delegate to ConnectionRegistry ---

    public TreeMap<String, ConnectionHandler> getConnections() {
        return connectionRegistry.getConnections();
    }

    public ConnectionHandler getConnectionHandler(Class clazz) {
        return connectionRegistry.getConnectionHandler(clazz);
    }

    public void addConnectionEvent(Consumer<ConnectionHandler> connectionHandler) {
        connectionRegistry.addConnectionEvent(connectionHandler);
    }

    // --- Lock Manager ---

    public LockManager getLockManager() {
        return lockManager;
    }

    public void setLockManager(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    // --- Shard Config ---

    public ShardConfig getShardConfig() {
        return shardConfig;
    }

    public void setShardConfig(ShardConfig shardConfig) {
        this.shardConfig = shardConfig;
    }

    // --- Startup synchronization ---

    public void waitTillReady() throws InterruptedException {
        CountDownLatch c;
        while ((c = getLatches().poll()) != null) {
            c.await();
        }
    }

    public Queue<CountDownLatch> getLatches() {
        return latches;
    }

    // --- Registries ---

    public TableRegistry getTableRegistry() {
        return tableRegistry;
    }

    public ConnectionRegistry getConnectionRegistry() {
        return connectionRegistry;
    }
}
