package com.nucleodb.grpc;

import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class NucleoDBGrpcServer {
    private static final Logger logger =
        Logger.getLogger(NucleoDBGrpcServer.class.getName());

    private final Server server;
    private final int port;

    NucleoDBGrpcServer(Server server, int port) {
        this.server = server;
        this.port = port;
    }

    public void start() throws IOException {
        server.start();
        logger.info("NucleoDB gRPC server started on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down gRPC server...");
            try {
                NucleoDBGrpcServer.this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            logger.info("gRPC server shut down.");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public int getPort() {
        return port;
    }

    public Server getServer() {
        return server;
    }

    public static NucleoDBGrpcServerBuilder builder() {
        return new NucleoDBGrpcServerBuilder();
    }
}
