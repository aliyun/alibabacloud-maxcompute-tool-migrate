package com.aliyun.odps.mma.util;

import lombok.Getter;

public class TableName {
    @Getter
    private final String dbName;
    @Getter
    private final String name;

    public TableName(String dbName, String name) {
        this.dbName = dbName;
        this.name = name;
    }

    public String toString() {
        return dbName + "." + name;
    }

    public String getFullName() {
        return toString();
    }
}

