package com.nucleodb.library;

import com.nucleodb.library.database.lock.LockConfig;
import com.nucleodb.library.database.tables.connection.ConnectionConsumer;
import com.nucleodb.library.database.tables.table.DataTableConsumer;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Consumer;

/**
 * Fluent builder for constructing NucleoDB instances.
 * <p>
 * Example usage:
 * <pre>
 * NucleoDB db = NucleoDBBuilder.create()
 *     .dbType(NucleoDB.DBType.ALL)
 *     .packages("com.example.models")
 *     .shardConfig(new ShardConfig(0, 3))
 *     .lockCustomizer(config -> config.setTopic("my-locks"))
 *     .build();
 * db.waitTillReady();
 * </pre>
 */
public class NucleoDBBuilder {
    private NucleoDB.DBType dbType = NucleoDB.DBType.ALL;
    private String[] packagesToScan;
    private String readToTime;
    private Consumer<LockConfig> lockCustomizer;
    private Consumer<DataTableConsumer> tableCustomizer;
    private Consumer<ConnectionConsumer> connectionCustomizer;
    private ShardConfig shardConfig;

    private NucleoDBBuilder() {}

    public static NucleoDBBuilder create() {
        return new NucleoDBBuilder();
    }

    public NucleoDBBuilder dbType(NucleoDB.DBType dbType) {
        this.dbType = dbType;
        return this;
    }

    public NucleoDBBuilder packages(String... packagesToScan) {
        this.packagesToScan = packagesToScan;
        return this;
    }

    public NucleoDBBuilder readToTime(String readToTime) {
        this.readToTime = readToTime;
        return this;
    }

    public NucleoDBBuilder lockCustomizer(Consumer<LockConfig> lockCustomizer) {
        this.lockCustomizer = lockCustomizer;
        return this;
    }

    public NucleoDBBuilder tableCustomizer(Consumer<DataTableConsumer> tableCustomizer) {
        this.tableCustomizer = tableCustomizer;
        return this;
    }

    public NucleoDBBuilder connectionCustomizer(Consumer<ConnectionConsumer> connectionCustomizer) {
        this.connectionCustomizer = connectionCustomizer;
        return this;
    }

    public NucleoDBBuilder shardConfig(ShardConfig shardConfig) {
        this.shardConfig = shardConfig;
        return this;
    }

    public NucleoDB build() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException,
            IntrospectionException, InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {
        return new NucleoDB(dbType, readToTime, connectionCustomizer, tableCustomizer, lockCustomizer, shardConfig, packagesToScan);
    }
}
