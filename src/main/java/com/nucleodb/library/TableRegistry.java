package com.nucleodb.library;

import com.nucleodb.library.database.index.annotation.Index;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.annotation.Table;
import com.nucleodb.library.database.tables.table.*;
import com.nucleodb.library.database.utils.StartupRun;
import com.nucleodb.library.database.utils.TreeSetExt;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import org.reflections.Reflections;

import java.beans.IntrospectionException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.nucleodb.library.utils.EnvReplace.replaceEnvVariables;
import static com.nucleodb.library.utils.field.FieldFinder.getAllAnnotatedFields;

/**
 * Manages the lifecycle of DataTables: scanning, configuring, building, and storing.
 */
public class TableRegistry {
    private static Logger logger = Logger.getLogger(TableRegistry.class.getName());

    private final TreeMap<String, DataTable> tables = new TreeMap<>();
    private final List<Consumer<DataTable>> tableEvents = new LinkedList<>();

    public Optional<Set<Class<?>>> getTableClasses(String[] packagesToScan) {
        return Arrays.stream(packagesToScan)
            .map(pkg -> new Reflections(replaceEnvVariables(pkg)).getTypesAnnotatedWith(Table.class))
            .reduce((a, b) -> { a.addAll(b); return a; });
    }

    public CountDownLatch startTables(
        NucleoDB nucleoDB,
        String[] packagesToScan,
        NucleoDB.DBType dbType,
        String readToTime,
        Consumer<DataTableConsumer> customizer,
        ShardConfig shardConfig
    ) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
        logger.info("NucleoDB Tables Starting");
        Optional<Set<Class<?>>> tableTypesOptional = getTableClasses(packagesToScan);
        if (tableTypesOptional.isEmpty()) {
            return null;
        }

        Set<Class<?>> tableTypes = tableTypesOptional.get();
        CountDownLatch latch = new CountDownLatch(tableTypes.size());
        Set<DataTableBuilder> builders = new TreeSetExt<>();
        Map<String, Set<DataTableConfig.IndexConfig>> indexes = new TreeMap<>();

        for (Class<?> type : tableTypes) {
            Table tableAnnotation = type.getAnnotation(Table.class);
            String tableName = tableAnnotation.tableName();
            if (tableName.isEmpty()) {
                tableName = type.getSimpleName().toLowerCase();
            }
            Class dataEntryClass = tableAnnotation.dataEntryClass();
            if (!DataEntry.class.isAssignableFrom(dataEntryClass)) {
                throw new IncorrectDataEntryClassException(
                    String.format("%s does not extend DataEntry", dataEntryClass.getName()));
            }
            try {
                if (dataEntryClass != DataEntry.class) {
                    dataEntryClass.getDeclaredConstructor(type);
                }
                dataEntryClass.getDeclaredConstructor(Create.class);
                dataEntryClass.getDeclaredConstructor();
                dataEntryClass.getDeclaredConstructor(String.class);
            } catch (NoSuchMethodException e) {
                throw new MissingDataEntryConstructorsException(
                    String.format("%s does not have all DataEntry constructors overridden!", dataEntryClass.getName()), e);
            }

            indexes.put(tableName, processIndexListForClass(type));

            StartupRun startupRun = new StartupRun() {
                public void run(DataTable table) {
                    latch.countDown();
                    tableEvents.forEach(listener -> listener.accept(table));
                }
            };

            DataTableBuilder builder = buildTable(tableName, dataEntryClass, type, dbType, startupRun, customizer, nucleoDB);
            if (shardConfig != null && shardConfig.getShardAssignment() != null) {
                builder.getConfig().setShardFilter(
                    new ShardFilter(shardConfig.getShardAssignment()));
            }
            builders.add(builder);
        }

        builders.forEach(builder -> {
            if (readToTime != null) {
                try {
                    builder.getConfig().setReadToTime(Instant.parse(readToTime));
                } catch (DateTimeParseException e) {
                    e.printStackTrace();
                }
            }
            builder.addIndexes(indexes.get(builder.getConfig().getTable()));
            try {
                DataTable built = builder.build();
                built.setNucleoDB(nucleoDB);
                built.setName(builder.getConfig().getTable());
            } catch (IntrospectionException | InvocationTargetException | NoSuchMethodException |
                     InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });

        logger.info("NucleoDB Tables ready to consume.");
        return latch;
    }

    public CountDownLatch startTable(
        NucleoDB nucleoDB,
        Class<?> type,
        NucleoDB.DBType dbType,
        String readToTime,
        Consumer<DataTableConsumer> customizer,
        ShardConfig shardConfig
    ) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
        Table tableAnnotation = type.getAnnotation(Table.class);
        CountDownLatch latch = new CountDownLatch(1);
        String tableName = tableAnnotation.tableName();
        Class dataEntryClass = tableAnnotation.dataEntryClass();
        if (!DataEntry.class.isAssignableFrom(dataEntryClass)) {
            throw new IncorrectDataEntryClassException(
                String.format("%s does not extend DataEntry", dataEntryClass.getName()));
        }
        if (!tableName.isEmpty()) {
            tableName = type.getSimpleName().toLowerCase();
        } else {
            throw new IncorrectDataEntryClassException(
                String.format("%s does not extend DataEntry", dataEntryClass.getName()));
        }
        logger.info("NucleoDB " + tableName + " Starting");

        try {
            if (dataEntryClass != DataEntry.class) {
                dataEntryClass.getDeclaredConstructor(type);
            }
            dataEntryClass.getDeclaredConstructor(Create.class);
            dataEntryClass.getDeclaredConstructor();
            dataEntryClass.getDeclaredConstructor(String.class);
        } catch (NoSuchMethodException e) {
            throw new MissingDataEntryConstructorsException(
                String.format("%s does not have all DataEntry constructors overridden!", dataEntryClass.getName()), e);
        }

        StartupRun startupRun = new StartupRun() {
            public void run(DataTable table) {
                latch.countDown();
                tableEvents.forEach(listener -> listener.accept(table));
            }
        };

        DataTableBuilder builder = buildTable(tableName, dataEntryClass, type, dbType, startupRun, customizer, nucleoDB);
        if (shardConfig != null && shardConfig.getShardAssignment() != null) {
            builder.getConfig().setShardFilter(
                new ShardFilter(shardConfig.getShardAssignment()));
        }

        if (readToTime != null) {
            try {
                builder.getConfig().setReadToTime(Instant.parse(readToTime));
            } catch (DateTimeParseException e) {
                e.printStackTrace();
            }
        }
        builder.addIndexes(processIndexListForClass(type));

        try {
            DataTable builtTable = builder.build();
            builtTable.setNucleoDB(nucleoDB);
            builtTable.setName(tableName);
        } catch (IntrospectionException | InvocationTargetException | NoSuchMethodException |
                 InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        logger.info("NucleoDB " + tableName + " ready to consume.");
        return latch;
    }

    private DataTableBuilder buildTable(
        String tableName,
        Class dataEntryClass,
        Class clazz,
        NucleoDB.DBType dbType,
        StartupRun startupRun,
        Consumer<DataTableConsumer> customizer,
        NucleoDB nucleoDB
    ) {
        DataTableBuilder builder = switch (dbType) {
            case ALL -> DataTableBuilder.create(tableName, clazz)
                .setDataEntryClass(dataEntryClass).setDb(nucleoDB).setStartupRun(startupRun);
            case NO_LOCAL -> DataTableBuilder.create(tableName, clazz)
                .setDataEntryClass(dataEntryClass).setLoadSave(false).setSaveChanges(false)
                .setDb(nucleoDB).setStartupRun(startupRun);
            case READ_ONLY -> DataTableBuilder.createReadOnly(tableName, clazz)
                .setDataEntryClass(dataEntryClass).setDb(nucleoDB).setStartupRun(startupRun);
            case EXPORT -> DataTableBuilder.create(tableName, clazz)
                .setDataEntryClass(dataEntryClass).setJSONExport(true)
                .setDb(nucleoDB).setStartupRun(startupRun);
        };
        if (customizer != null) {
            customizer.accept(new DataTableConsumer(clazz, builder.getConfig()));
        }
        return builder;
    }

    Set<DataTableConfig.IndexConfig> processIndexListForClass(Class<?> clazz) {
        Set<DataTableConfig.IndexConfig> indexes = new TreeSet<>();
        getAllAnnotatedFields(clazz, Index.class, "").forEach(field -> {
            if (field.getAnnotation().value().isEmpty()) {
                indexes.add(new DataTableConfig.IndexConfig(field.getPath(), field.getAnnotation().type()));
            } else {
                indexes.add(new DataTableConfig.IndexConfig(field.getAnnotation().value(), field.getAnnotation().type()));
            }
        });
        return indexes;
    }

    public DataTable getTable(String name) {
        return tables.get(name);
    }

    public DataTable getTable(Class clazz) {
        Annotation annotation = clazz.getAnnotation(Table.class);
        if (annotation != null) {
            return tables.get(((Table) annotation).tableName());
        }
        return null;
    }

    public TreeMap<String, DataTable> getTables() {
        return tables;
    }

    public void addTableEvent(Consumer<DataTable> event) {
        tableEvents.add(event);
    }
}
