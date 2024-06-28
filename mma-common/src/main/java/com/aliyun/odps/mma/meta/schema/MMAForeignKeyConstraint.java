package com.aliyun.odps.mma.meta.schema;

import lombok.Getter;

import java.util.List;

@Getter
public class MMAForeignKeyConstraint {
    private List<String> childColumns;
    private String name;
    private List<String> parentColumns;
    private String parentTable;

    public MMAForeignKeyConstraint(List<String> childColumns, String name, List<String> parentColumns, String parentTable) {
        this.childColumns = childColumns;
        this.name = name;
        this.parentColumns = parentColumns;
        this.parentTable = parentTable;
    }
}
