package com.nucleodb.grpc.service;

import com.nucleodb.grpc.proto.*;
import com.nucleodb.grpc.util.JsonHelper;
import com.nucleodb.grpc.util.StatusHelper;
import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Pagination;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataEntryService extends DataEntryServiceGrpc.DataEntryServiceImplBase {
    private static final Logger logger = Logger.getLogger(DataEntryService.class.getName());

    private final NucleoDB nucleoDB;

    public DataEntryService(NucleoDB nucleoDB) {
        this.nucleoDB = nucleoDB;
    }

    @Override
    public void create(CreateDataEntryRequest request,
                       StreamObserver<CreateDataEntryResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(CreateDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Class<?> dataClass = table.getConfig().getClazz();
            Object dataObj = JsonHelper.mapper().readValue(request.getDataJson(), dataClass);

            Class<?> deClass = table.getConfig().getDataEntryClass();
            DataEntry entry;
            if (deClass == DataEntry.class) {
                entry = new DataEntry(dataObj);
            } else {
                entry = (DataEntry) deClass.getDeclaredConstructor(dataClass)
                    .newInstance(dataObj);
            }

            table.saveSync(entry);

            responseObserver.onNext(CreateDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .setEntry(JsonHelper.toProto(entry))
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Create", e);
            responseObserver.onNext(CreateDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void get(GetDataEntryRequest request,
                    StreamObserver<GetDataEntryResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(GetDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Set<DataEntry> entries = table.get("id", request.getKey(), null);
            if (entries == null || entries.isEmpty()) {
                responseObserver.onNext(GetDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.ENTRY_NOT_FOUND,
                        "Entry '" + request.getKey() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            DataEntry entry = entries.iterator().next();
            responseObserver.onNext(GetDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .setEntry(JsonHelper.toProto(entry))
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Get", e);
            responseObserver.onNext(GetDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getByIndex(GetDataEntriesByIndexRequest request,
                           StreamObserver<GetDataEntriesByIndexResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(GetDataEntriesByIndexResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Object value = JsonHelper.mapper().readValue(request.getValueJson(), Object.class);

            DataEntryProjection projection = new DataEntryProjection();
            if (request.hasPagination()) {
                projection = new DataEntryProjection(new Pagination(
                    request.getPagination().getSkip(),
                    request.getPagination().getLimit()));
            }

            Set<DataEntry> entries = table.get(request.getIndexKey(), value, projection);

            GetDataEntriesByIndexResponse.Builder resp =
                GetDataEntriesByIndexResponse.newBuilder()
                    .setStatus(StatusHelper.success());

            if (entries != null) {
                for (DataEntry entry : entries) {
                    resp.addEntries(JsonHelper.toProto(entry));
                }
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in GetByIndex", e);
            responseObserver.onNext(GetDataEntriesByIndexResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getAll(GetAllDataEntriesRequest request,
                       StreamObserver<GetAllDataEntriesResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(GetAllDataEntriesResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Set<DataEntry> allEntries = table.getEntries();
            long totalCount = allEntries.size();

            long skip = 0, limit = 50;
            if (request.hasPagination()) {
                skip = request.getPagination().getSkip();
                limit = request.getPagination().getLimit();
            }

            GetAllDataEntriesResponse.Builder resp =
                GetAllDataEntriesResponse.newBuilder()
                    .setStatus(StatusHelper.success())
                    .setTotalCount(totalCount);

            long count = 0;
            long skipped = 0;
            for (DataEntry entry : allEntries) {
                if (skipped < skip) {
                    skipped++;
                    continue;
                }
                if (count >= limit) break;
                resp.addEntries(JsonHelper.toProto(entry));
                count++;
            }

            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in GetAll", e);
            responseObserver.onNext(GetAllDataEntriesResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void update(UpdateDataEntryRequest request,
                       StreamObserver<UpdateDataEntryResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(UpdateDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            DataEntryProjection projection = new DataEntryProjection();
            projection.setWritable(true);
            Set<DataEntry> entries = table.get("id", request.getKey(), projection);

            if (entries == null || entries.isEmpty()) {
                responseObserver.onNext(UpdateDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.ENTRY_NOT_FOUND,
                        "Entry '" + request.getKey() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            DataEntry entryCopy = entries.iterator().next();

            Class<?> dataClass = table.getConfig().getClazz();
            Object newData = JsonHelper.mapper().readValue(request.getDataJson(), dataClass);
            entryCopy.setData(newData);

            table.saveSync(entryCopy);

            responseObserver.onNext(UpdateDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .setEntry(JsonHelper.toProto(entryCopy))
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Update", e);
            responseObserver.onNext(UpdateDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void delete(DeleteDataEntryRequest request,
                       StreamObserver<DeleteDataEntryResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(DeleteDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            DataEntryProjection projection = new DataEntryProjection();
            projection.setWritable(true);
            Set<DataEntry> entries = table.get("id", request.getKey(), projection);

            if (entries == null || entries.isEmpty()) {
                responseObserver.onNext(DeleteDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.ENTRY_NOT_FOUND,
                        "Entry '" + request.getKey() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            DataEntry entry = entries.iterator().next();
            table.deleteSync(entry);

            responseObserver.onNext(DeleteDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Delete", e);
            responseObserver.onNext(DeleteDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void executeSql(SqlRequest request,
                           StreamObserver<SqlResponse> responseObserver) {
        try {
            Object result;
            if (request.getResultClass() != null && !request.getResultClass().isEmpty()) {
                Class<?> clazz = Class.forName(request.getResultClass());
                result = nucleoDB.sql(request.getSql(), clazz);
            } else {
                result = nucleoDB.sql(request.getSql());
            }

            String resultJson = JsonHelper.toJson(result);
            responseObserver.onNext(SqlResponse.newBuilder()
                .setStatus(StatusHelper.success())
                .setResultJson(resultJson)
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in ExecuteSql", e);
            responseObserver.onNext(SqlResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void bulkCreate(BulkCreateDataEntryRequest request,
                           StreamObserver<BulkCreateDataEntryResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(BulkCreateDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            Class<?> dataClass = table.getConfig().getClazz();
            Class<?> deClass = table.getConfig().getDataEntryClass();

            List<DataEntryMessage> created = new ArrayList<>();
            int successCount = 0;
            int failureCount = 0;

            for (String dataJson : request.getDataJsonListList()) {
                try {
                    Object dataObj = JsonHelper.mapper().readValue(dataJson, dataClass);
                    DataEntry entry;
                    if (deClass == DataEntry.class) {
                        entry = new DataEntry(dataObj);
                    } else {
                        entry = (DataEntry) deClass
                            .getDeclaredConstructor(dataClass)
                            .newInstance(dataObj);
                    }
                    table.saveSync(entry);
                    created.add(JsonHelper.toProto(entry));
                    successCount++;
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Bulk create item failed", e);
                    failureCount++;
                }
            }

            responseObserver.onNext(BulkCreateDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.success(
                    successCount + " created, " + failureCount + " failed"))
                .addAllEntries(created)
                .setSuccessCount(successCount)
                .setFailureCount(failureCount)
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in BulkCreate", e);
            responseObserver.onNext(BulkCreateDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void bulkDelete(BulkDeleteDataEntryRequest request,
                           StreamObserver<BulkDeleteDataEntryResponse> responseObserver) {
        try {
            DataTable table = nucleoDB.getTable(request.getTable());
            if (table == null) {
                responseObserver.onNext(BulkDeleteDataEntryResponse.newBuilder()
                    .setStatus(StatusHelper.error(StatusHelper.TABLE_NOT_FOUND,
                        "Table '" + request.getTable() + "' not found"))
                    .build());
                responseObserver.onCompleted();
                return;
            }

            int successCount = 0;
            int failureCount = 0;

            for (String key : request.getKeysList()) {
                try {
                    DataEntryProjection projection = new DataEntryProjection();
                    projection.setWritable(true);
                    Set<DataEntry> entries = table.get("id", key, projection);
                    if (entries != null && !entries.isEmpty()) {
                        table.deleteSync(entries.iterator().next());
                        successCount++;
                    } else {
                        failureCount++;
                    }
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Bulk delete item failed for key: " + key, e);
                    failureCount++;
                }
            }

            responseObserver.onNext(BulkDeleteDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.success(
                    successCount + " deleted, " + failureCount + " failed"))
                .setSuccessCount(successCount)
                .setFailureCount(failureCount)
                .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in BulkDelete", e);
            responseObserver.onNext(BulkDeleteDataEntryResponse.newBuilder()
                .setStatus(StatusHelper.error(StatusHelper.INTERNAL_ERROR, e.getMessage()))
                .build());
            responseObserver.onCompleted();
        }
    }
}
