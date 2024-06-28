package com.aliyun.odps.mma.meta.schema;

public class SchemaAdapterError extends RuntimeException {
    public SchemaAdapterError(String message) {
        super(message);
    }

    public SchemaAdapterError(String message, Exception e) {
        super(message, e);
    }
}
