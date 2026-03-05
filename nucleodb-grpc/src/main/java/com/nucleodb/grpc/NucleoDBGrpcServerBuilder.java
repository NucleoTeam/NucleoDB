package com.nucleodb.grpc;

import com.nucleodb.grpc.service.ConnectionService;
import com.nucleodb.grpc.service.DataEntryService;
import com.nucleodb.library.NucleoDB;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.util.ArrayList;
import java.util.List;

public class NucleoDBGrpcServerBuilder {

    private NucleoDB nucleoDB;
    private int port = 9090;
    private boolean enableReflection = true;
    private final List<ServerInterceptor> interceptors = new ArrayList<>();

    NucleoDBGrpcServerBuilder() {}

    public NucleoDBGrpcServerBuilder nucleoDB(NucleoDB nucleoDB) {
        this.nucleoDB = nucleoDB;
        return this;
    }

    public NucleoDBGrpcServerBuilder port(int port) {
        this.port = port;
        return this;
    }

    public NucleoDBGrpcServerBuilder enableReflection(boolean enable) {
        this.enableReflection = enable;
        return this;
    }

    public NucleoDBGrpcServerBuilder addInterceptor(ServerInterceptor interceptor) {
        this.interceptors.add(interceptor);
        return this;
    }

    public NucleoDBGrpcServer build() {
        if (nucleoDB == null) {
            throw new IllegalStateException("NucleoDB instance is required");
        }

        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port)
            .addService(new DataEntryService(nucleoDB))
            .addService(new ConnectionService(nucleoDB));

        if (enableReflection) {
            serverBuilder.addService(ProtoReflectionService.newInstance());
        }

        for (ServerInterceptor interceptor : interceptors) {
            serverBuilder.intercept(interceptor);
        }

        Server server = serverBuilder.build();
        return new NucleoDBGrpcServer(server, port);
    }
}
