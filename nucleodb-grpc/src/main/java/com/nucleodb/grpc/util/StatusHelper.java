package com.nucleodb.grpc.util;

import com.nucleodb.grpc.proto.OperationStatus;

public final class StatusHelper {

    private StatusHelper() {}

    public static OperationStatus success() {
        return OperationStatus.newBuilder()
            .setSuccess(true)
            .setMessage("OK")
            .build();
    }

    public static OperationStatus success(String message) {
        return OperationStatus.newBuilder()
            .setSuccess(true)
            .setMessage(message)
            .build();
    }

    public static OperationStatus error(String errorCode, String message) {
        return OperationStatus.newBuilder()
            .setSuccess(false)
            .setErrorCode(errorCode)
            .setMessage(message)
            .build();
    }

    public static final String TABLE_NOT_FOUND = "TABLE_NOT_FOUND";
    public static final String CONNECTION_TYPE_NOT_FOUND = "CONNECTION_TYPE_NOT_FOUND";
    public static final String ENTRY_NOT_FOUND = "ENTRY_NOT_FOUND";
    public static final String CONNECTION_NOT_FOUND = "CONNECTION_NOT_FOUND";
    public static final String INVALID_REQUEST = "INVALID_REQUEST";
    public static final String INTERNAL_ERROR = "INTERNAL_ERROR";
    public static final String SERIALIZATION_ERROR = "SERIALIZATION_ERROR";
}
