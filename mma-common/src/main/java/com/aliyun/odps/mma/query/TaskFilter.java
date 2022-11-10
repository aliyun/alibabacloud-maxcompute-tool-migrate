package com.aliyun.odps.mma.query;

import com.aliyun.odps.mma.constant.TaskStatus;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Data
public class TaskFilter extends QueryFilter {
    private int sourceId;
    private int jobId;
    private List<Integer> jobIds;
    private List<Integer> taskIds;
    private String jobName;
    private String dbName;
    private String tableName;
    private String sourceName;
    private String odpsProject;
    private String odpsTable;
    private String status;
    private Boolean allFailedStatus;
    private int running;
    private String partition;
    private int stopped;
    private TaskSorter sorter;

    public List<SortPair> getOrders() {
        if (Objects.nonNull(sorter)) {
            return sorter.orders();
        }

        return Collections.emptyList();
    }

    public void setStatus(String status) {
        if ("STOPPED".equals(status)) {
            this.status = null;
            this.stopped = 1;
            return;
        }

        if ("RUNNING".equals(status)) {
            this.status = null;
            this.running = 1;
            return;
        }

        if ("FAILED".equals(status)) {
            this.status = null;
            this.allFailedStatus = true;
            return;
        }

        this.status = status;
    }

    @Data
    public static class TaskSorter extends Sorter {
        @FiledName("start_time")
        private String startTime;
        @FiledName("status")
        private String status;
    }
}
