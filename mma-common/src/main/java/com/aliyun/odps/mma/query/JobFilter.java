package com.aliyun.odps.mma.query;

import lombok.Data;

import java.util.List;

@Data
public class JobFilter extends QueryFilter {
    private String sourceName;
    private String dbName;
    private String tableName;
    private String dstOdpsProject;
    private String dstOdpsTable;
    private String ptValue;
    private int dbId;
    private int tableId;
    private int taskId;
    private String status;
    private int stopped;
    private List<Integer> jobIds;

    public boolean hasNoConditionsForTask() {
        return dbName == null && tableName == null && ptValue == null;
    }

    public void setStatus(String status) {
        if (!"STOPPED".equals(status)) {
            this.status = status;
            return;
        }

        this.status = null;
        this.stopped = 1;
    }
}
