package com.nucleodb.example;

import com.nucleodb.grpc.NucleoDBGrpcServer;
import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.NucleoDBBuilder;
import com.nucleodb.library.mqs.local.LocalConfiguration;

import java.util.logging.Logger;

/**
 * Example NucleoDB gRPC server.
 *
 * Supports two MQS modes controlled by the MQS_MODE environment variable:
 *   - "kafka" (default): uses Kafka via KafkaSettings (127.0.0.1:19092,29092,39092)
 *   - "local": uses in-process LocalConfiguration (no external dependencies)
 *
 * Usage:
 *   java -cp ... com.nucleodb.example.ExampleServer [grpcPort]
 *
 * Environment:
 *   MQS_MODE=local|kafka (default: kafka)
 */
public class ExampleServer {
    private static final Logger logger = Logger.getLogger(ExampleServer.class.getName());

    public static NucleoDBGrpcServer startServer(int port, String mqsMode) throws Exception {
        boolean useLocal = "local".equalsIgnoreCase(mqsMode);

        logger.info("Starting NucleoDB with " + (useLocal ? "Local" : "Kafka") + " MQS on port " + port + "...");

        NucleoDBBuilder builder = NucleoDBBuilder.create()
            .dbType(NucleoDB.DBType.ALL)
            .packages("com.nucleodb.example.models");

        if (useLocal) {
            builder.connectionCustomizer(c -> {
                c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration());
                c.getConnectionConfig().setLoadSaved(false);
                c.getConnectionConfig().setSaveChanges(false);
            });
            builder.tableCustomizer(c -> {
                c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration());
                c.getDataTableConfig().setLoadSave(false);
                c.getDataTableConfig().setSaveChanges(false);
            });
            builder.lockCustomizer(c ->
                c.setMqsConfiguration(new LocalConfiguration()));
        }

        NucleoDB nucleoDB = builder.build();
        nucleoDB.waitTillReady();
        logger.info("NucleoDB is ready. Tables: " + nucleoDB.getTables().keySet()
            + ", Connections: " + nucleoDB.getConnections().keySet());

        NucleoDBGrpcServer grpcServer = NucleoDBGrpcServer.builder()
            .nucleoDB(nucleoDB)
            .port(port)
            .build();
        grpcServer.start();

        logger.info("gRPC server listening on port " + port);
        return grpcServer;
    }

    public static void main(String[] args) throws Exception {
        int port = 9090;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        String mqsMode = System.getenv().getOrDefault("MQS_MODE", "kafka");

        NucleoDBGrpcServer server = startServer(port, mqsMode);
        server.blockUntilShutdown();
    }
}
