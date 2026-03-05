package com.nucleodb.grpc.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nucleodb.grpc.proto.ConnectionMessage;
import com.nucleodb.grpc.proto.DataEntryMessage;
import com.nucleodb.library.database.tables.connection.Connection;
import com.nucleodb.library.database.tables.table.DataEntry;

public final class JsonHelper {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonHelper() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static DataEntryMessage toProto(DataEntry<?> entry) throws JsonProcessingException {
        DataEntryMessage.Builder builder = DataEntryMessage.newBuilder()
            .setKey(entry.getKey())
            .setVersion(entry.getVersion())
            .setDataJson(MAPPER.writeValueAsString(entry.getData()));
        if (entry.getCreated() != null) {
            builder.setCreated(entry.getCreated().toString());
        }
        if (entry.getModified() != null) {
            builder.setModified(entry.getModified().toString());
        }
        return builder.build();
    }

    public static ConnectionMessage toProto(Connection<?, ?> conn) {
        ConnectionMessage.Builder builder = ConnectionMessage.newBuilder()
            .setUuid(conn.getUuid())
            .setFromKey(conn.getFromKey())
            .setToKey(conn.getToKey())
            .setVersion(conn.getVersion());
        if (conn.getMetadata() != null) {
            builder.putAllMetadata(conn.getMetadata());
        }
        if (conn.getDate() != null) {
            builder.setDate(conn.getDate().toString());
        }
        if (conn.getModified() != null) {
            builder.setModified(conn.getModified().toString());
        }
        return builder.build();
    }

    public static String toJson(Object obj) throws JsonProcessingException {
        return MAPPER.writeValueAsString(obj);
    }
}
