package com.aliyun.odps.mma.meta.schema;

import lombok.Getter;

import java.util.List;

@Getter
public class MMATableConstraint {
    private final List<String> primaryKeys;
    private final MMAForeignKeyConstraint foreignKeyConstraint;

    public MMATableConstraint(List<String> primaryKeys, MMAForeignKeyConstraint foreignKeyConstraint) {
        this.primaryKeys = primaryKeys;
        this.foreignKeyConstraint = foreignKeyConstraint;
    }

    public MMATableConstraint(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        this.foreignKeyConstraint = null;
    }
}
