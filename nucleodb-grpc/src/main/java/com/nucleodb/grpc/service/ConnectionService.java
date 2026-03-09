package com.nucleodb.grpc.service;

import com.nucleodb.grpc.proto.*;
import com.nucleodb.grpc.util.JsonHelper;
import com.nucleodb.grpc.util.StatusHelper;
import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.tables.connection.Connection;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.connection.ConnectionProjection;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.utils.Pagination;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionService extends ConnectionServiceGrpc.ConnectionServiceImplBase {
    private static final Logger logger = Logger.getLogger(ConnectionService.class.getName());

    private final NucleoDB nucleoDB;

    public ConnectionService(NucleoDB nucleoDB) {
        this.nucleoDB = nucleoDB;
    }

    private ConnectionHandler<?> resolveHandler(String connectionType) {
        return nucleoDB.getConnections().get(connectionType.toUpperCase());
    }

    private DataEntry<?> stubEntry(String key) {
        return new DataEntry<>(key);
    }

    @Override
    public void create(CreateConnectionRequest request,
                       StreamObserver<CreateConnectionResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(CreateConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type '" + request.getConnectionType() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Connection connection = (Connection) handler.getConfig()
                .getConnectionClass().getDeclaredConstructor().newInstance();
            connection.setFromKey(request.getFromKey());
            connection.setToKey(request.getToKey());
            if (request.getMetadataMap() != null && !request.getMetadataMap().isEmpty()) {
                connection.getMetadata().putAll(request.getMetadataMap());
            }

            handler.saveSync(connection);

            responseObserver.onNext(CreateConnectionResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .setConnection(JsonHelper.toProto(connection))
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection Create", e);
            responseObserver.onNext(CreateConnectionResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void get(GetConnectionRequest request,
                    StreamObserver<GetConnectionResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(GetConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type '" + request.getConnectionType() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Connection connection = (Connection) handler
                .getConnectionByUUID().get(request.getUuid());
            if (connection == null) {
                responseObserver.onNext(GetConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_NOT_FOUND,
                        "Connection '" + request.getUuid() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            responseObserver.onNext(GetConnectionResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .setConnection(JsonHelper.toProto(connection))
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection Get", e);
            responseObserver.onNext(GetConnectionResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getByFrom(GetConnectionsByFromRequest request,
                          StreamObserver<GetConnectionsByFromResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(GetConnectionsByFromResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type '" + request.getConnectionType() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            ConnectionProjection projection = new ConnectionProjection<>();
            if (request.hasPagination()) {
                projection = new ConnectionProjection<>(new Pagination(
                    request.getPagination().getSkip(),
                    request.getPagination().getLimit()));
            }

            Set<Connection> connections = handler.getByFrom(
                stubEntry(request.getFromKey()), projection);

            GetConnectionsByFromResponse.Builder resp =
                GetConnectionsByFromResponse.newBuilder()
                    .setStatus(StatusHelper.success());

            if (connections != null) {
                for (Connection conn : connections) {
                    resp.addConnections(JsonHelper.toProto(conn));
                }
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection GetByFrom", e);
            responseObserver.onNext(GetConnectionsByFromResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getByFromAndTo(GetConnectionsByFromAndToRequest request,
                               StreamObserver<GetConnectionsByFromAndToResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(GetConnectionsByFromAndToResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            ConnectionProjection projection = new ConnectionProjection<>();
            if (request.hasPagination()) {
                projection = new ConnectionProjection<>(new Pagination(
                    request.getPagination().getSkip(),
                    request.getPagination().getLimit()));
            }

            Set<Connection> connections = handler.getByFromAndTo(
                stubEntry(request.getFromKey()),
                stubEntry(request.getToKey()),
                projection);

            GetConnectionsByFromAndToResponse.Builder resp =
                GetConnectionsByFromAndToResponse.newBuilder()
                    .setStatus(StatusHelper.success());

            if (connections != null) {
                for (Connection conn : connections) {
                    resp.addConnections(JsonHelper.toProto(conn));
                }
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection GetByFromAndTo", e);
            responseObserver.onNext(GetConnectionsByFromAndToResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getByTo(GetConnectionsByToRequest request,
                        StreamObserver<GetConnectionsByToResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(GetConnectionsByToResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            ConnectionProjection projection = new ConnectionProjection<>();
            if (request.hasPagination()) {
                projection = new ConnectionProjection<>(new Pagination(
                    request.getPagination().getSkip(),
                    request.getPagination().getLimit()));
            }

            Set<Connection> connections = handler.getReverseByTo(
                stubEntry(request.getToKey()), projection);

            GetConnectionsByToResponse.Builder resp =
                GetConnectionsByToResponse.newBuilder()
                    .setStatus(StatusHelper.success());

            if (connections != null) {
                for (Connection conn : connections) {
                    resp.addConnections(JsonHelper.toProto(conn));
                }
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection GetByTo", e);
            responseObserver.onNext(GetConnectionsByToResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getAll(GetAllConnectionsRequest request,
                       StreamObserver<GetAllConnectionsResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(GetAllConnectionsResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            long totalCount = handler.getAllConnections().size();

            ConnectionProjection projection = new ConnectionProjection<>();
            if (request.hasPagination()) {
                projection = new ConnectionProjection<>(new Pagination(
                    request.getPagination().getSkip(),
                    request.getPagination().getLimit()));
            }

            Set<Connection> connections = handler.get(projection);

            GetAllConnectionsResponse.Builder resp =
                GetAllConnectionsResponse.newBuilder()
                    .setStatus(StatusHelper.success())
                    .setTotalCount(totalCount);

            if (connections != null) {
                for (Connection conn : connections) {
                    resp.addConnections(JsonHelper.toProto(conn));
                }
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection GetAll", e);
            responseObserver.onNext(GetAllConnectionsResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void update(UpdateConnectionRequest request,
                       StreamObserver<UpdateConnectionResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(UpdateConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Connection original = (Connection) handler
                .getConnectionByUUID().get(request.getUuid());
            if (original == null) {
                responseObserver.onNext(UpdateConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_NOT_FOUND,
                        "Connection '" + request.getUuid() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Connection copy = original.copy(
                handler.getConfig().getConnectionClass(), false);

            if (request.getMetadataMap() != null) {
                copy.getMetadata().clear();
                copy.getMetadata().putAll(request.getMetadataMap());
            }

            handler.saveSync(copy);

            responseObserver.onNext(UpdateConnectionResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .setConnection(JsonHelper.toProto(copy))
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection Update", e);
            responseObserver.onNext(UpdateConnectionResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void delete(DeleteConnectionRequest request,
                       StreamObserver<DeleteConnectionResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(DeleteConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Connection original = (Connection) handler
                .getConnectionByUUID().get(request.getUuid());
            if (original == null) {
                responseObserver.onNext(DeleteConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_NOT_FOUND,
                        "Connection '" + request.getUuid() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Connection copy = original.copy(
                handler.getConfig().getConnectionClass(), true);
            handler.deleteSync(copy);

            responseObserver.onNext(DeleteConnectionResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection Delete", e);
            responseObserver.onNext(DeleteConnectionResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void bulkCreate(BulkCreateConnectionRequest request,
                           StreamObserver<BulkCreateConnectionResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(BulkCreateConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            List<ConnectionMessage> created = new ArrayList<>();
            int successCount = 0;
            int failureCount = 0;

            for (CreateConnectionEntry entry : request.getConnectionsList()) {
                try {
                    Connection conn = (Connection) handler.getConfig()
                        .getConnectionClass().getDeclaredConstructor().newInstance();
                    conn.setFromKey(entry.getFromKey());
                    conn.setToKey(entry.getToKey());
                    if (entry.getMetadataMap() != null) {
                        conn.getMetadata().putAll(entry.getMetadataMap());
                    }
                    handler.saveSync(conn);
                    created.add(JsonHelper.toProto(conn));
                    successCount++;
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Bulk connection create item failed", e);
                    failureCount++;
                }
            }

            responseObserver.onNext(BulkCreateConnectionResponse.newBuilder()
                .setStatus(StatusHelper.success(
                    successCount + " created, " + failureCount + " failed"))
                .addAllConnections(created)
                .setSuccessCount(successCount)
                .setFailureCount(failureCount)
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection BulkCreate", e);
            responseObserver.onNext(BulkCreateConnectionResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void bulkDelete(BulkDeleteConnectionRequest request,
                           StreamObserver<BulkDeleteConnectionResponse> responseObserver) {
        try {
            ConnectionHandler handler = resolveHandler(request.getConnectionType());
            if (handler == null) {
                responseObserver.onNext(BulkDeleteConnectionResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.CONNECTION_TYPE_NOT_FOUND,
                        "Connection type not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            int successCount = 0;
            int failureCount = 0;

            for (String uuid : request.getUuidsList()) {
                try {
                    Connection conn = (Connection) handler
                        .getConnectionByUUID().get(uuid);
                    if (conn != null) {
                        Connection connCopy = conn.copy(
                            handler.getConfig().getConnectionClass(), true);
                        handler.deleteSync(connCopy);
                        successCount++;
                    } else {
                        failureCount++;
                    }
                } catch (Exception e) {
                    logger.log(Level.WARNING,
                        "Bulk connection delete failed for uuid: " + uuid, e);
                    failureCount++;
                }
            }

            responseObserver.onNext(BulkDeleteConnectionResponse.newBuilder()
                .setStatus(StatusHelper.success(
                    successCount + " deleted, " + failureCount + " failed"))
                .setSuccessCount(successCount)
                .setFailureCount(failureCount)
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Connection BulkDelete", e);
            responseObserver.onNext(BulkDeleteConnectionResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }
}
