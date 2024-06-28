package com.aliyun.odps.mma.util;

import lombok.Getter;

public class TableName {
    @Getter
    private final String dbName;
    @Getter
    private final String schemaName;
    @Getter
    private final String name;

    public TableName(String dbName, String schemaName, String name) {
        this.dbName = dbName;
        this.schemaName = schemaName;
        this.name = name;
    }

    public String toString() {
        if (schemaName == null) {
            return dbName + "." + name;
        } else {
            return dbName + "." + schemaName + "." + name;
        }
    }

    public String getFullName() {
        return toString();
    }
}

