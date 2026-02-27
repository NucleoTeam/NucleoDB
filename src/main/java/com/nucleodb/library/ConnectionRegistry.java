package com.nucleodb.library;

import com.nucleodb.library.database.tables.annotation.Conn;
import com.nucleodb.library.database.tables.connection.*;
import com.nucleodb.library.database.utils.StartupRun;
import org.reflections.Reflections;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.nucleodb.library.utils.EnvReplace.replaceEnvVariables;

/**
 * Manages the lifecycle of ConnectionHandlers: scanning, configuring, building, and storing.
 */
public class ConnectionRegistry {
    private static Logger logger = Logger.getLogger(ConnectionRegistry.class.getName());

    private final TreeMap<String, ConnectionHandler> connections = new TreeMap<>();
    private final List<Consumer<ConnectionHandler>> connectionEvents = new LinkedList<>();

    public Optional<Set<Class<?>>> getConnectionClasses(String[] packagesToScan) {
        return Arrays.stream(packagesToScan)
            .map(pkg -> new Reflections(replaceEnvVariables(pkg)).getTypesAnnotatedWith(Conn.class))
            .reduce((a, b) -> { a.addAll(b); return a; });
    }

    public CountDownLatch startConnections(
        NucleoDB nucleoDB,
        String[] packagesToScan,
        NucleoDB.DBType dbType,
        String readToTime,
        Consumer<ConnectionConsumer> customizer,
        ShardConfig shardConfig
    ) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        logger.info("NucleoDB Connections Starting");
        Optional<Set<Class<?>>> connectionTypesOptional = getConnectionClasses(packagesToScan);
        if (connectionTypesOptional.isEmpty()) {
            return null;
        }

        Set<Class<?>> connectionTypes = connectionTypesOptional.get();
        CountDownLatch latch = new CountDownLatch(connectionTypes.size());

        for (Class<?> type : connectionTypes) {
            buildAndRegisterConnection(nucleoDB, type, dbType, readToTime, customizer, shardConfig, latch);
        }

        logger.info("NucleoDB connections ready to consume.");
        return latch;
    }

    public CountDownLatch startConnection(
        NucleoDB nucleoDB,
        Class<?> type,
        NucleoDB.DBType dbType,
        String readToTime,
        Consumer<ConnectionConsumer> customizer,
        ShardConfig shardConfig
    ) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        CountDownLatch latch = new CountDownLatch(1);
        buildAndRegisterConnection(nucleoDB, type, dbType, readToTime, customizer, shardConfig, latch);
        return latch;
    }

    private void buildAndRegisterConnection(
        NucleoDB nucleoDB,
        Class<?> type,
        NucleoDB.DBType dbType,
        String readToTime,
        Consumer<ConnectionConsumer> customizer,
        ShardConfig shardConfig,
        CountDownLatch latch
    ) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Conn connectionType = type.getAnnotation(Conn.class);
        String topic = String.format("%ss", connectionType.value().toLowerCase());
        logger.info("NucleoDB Connection[" + connectionType.value() + "] Starting");

        ConnectionConfig config = new ConnectionConfig();
        config.setTopic(topic);

        Type[] actualTypeArguments = ((ParameterizedType) type.getGenericSuperclass()).getActualTypeArguments();
        Type[] toTableTypeArguments = new Type[0];
        Type[] fromTableTypeArguments = new Type[0];
        if (actualTypeArguments.length == 2) {
            Class<?> toTable = (Class<?>) actualTypeArguments[1];
            toTableTypeArguments = ((ParameterizedType) toTable.getGenericSuperclass()).getActualTypeArguments();
            Class<?> fromTable = (Class<?>) actualTypeArguments[0];
            fromTableTypeArguments = ((ParameterizedType) fromTable.getGenericSuperclass()).getActualTypeArguments();
        }
        if (toTableTypeArguments.length != 1 && fromTableTypeArguments.length != 1) {
            System.exit(1);
        }

        config.setToTable((Class<?>) toTableTypeArguments[0]);
        logger.info("To Table " + config.getToTable().getName());
        config.setFromTable((Class<?>) fromTableTypeArguments[0]);
        logger.info("From table " + config.getFromTable().getName());
        config.setConnectionClass(type);

        if (readToTime != null) {
            try {
                config.setReadToTime(Instant.parse(readToTime));
            } catch (DateTimeParseException e) {
                e.printStackTrace();
            }
        }

        config.setLabel(connectionType.value().toUpperCase());

        if (customizer != null) {
            customizer.accept(new ConnectionConsumer(toTableTypeArguments[0], fromTableTypeArguments[0], config));
        }

        if (shardConfig != null && shardConfig.getShardAssignment() != null) {
            config.setShardFilter(new ShardFilter(shardConfig.getShardAssignment()));
        }

        config.getStartupRuns().add(new StartupRun() {
            public void run(ConnectionHandler connectionHandler) {
                latch.countDown();
                connectionEvents.forEach(listener -> listener.accept(connectionHandler));
            }
        });

        switch (dbType) {
            case NO_LOCAL -> {
                config.setSaveChanges(false);
                config.setLoadSaved(false);
            }
            case READ_ONLY -> config.setWrite(false);
            case EXPORT -> config.setJsonExport(true);
        }

        ConnectionHandler connectionHandler = new ConnectionHandler(nucleoDB, config);
        connectionHandler.setName(connectionType.value().toUpperCase());
        connections.put(connectionHandler.getName(), connectionHandler);
        logger.info("NucleoDB Connection[" + connectionType.value() + "] ready to consume.");
    }

    public ConnectionHandler getConnectionHandler(Class clazz) {
        if (!clazz.isAnnotationPresent(Conn.class)) {
            return null;
        }
        Conn conn = (Conn) clazz.getDeclaredAnnotation(Conn.class);
        return connections.get(conn.value().toUpperCase());
    }

    public TreeMap<String, ConnectionHandler> getConnections() {
        return connections;
    }

    public void addConnectionEvent(Consumer<ConnectionHandler> event) {
        connectionEvents.add(event);
    }
}
