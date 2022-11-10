package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.constant.TaskType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskModel {
    private Integer id;
    private Integer jobId;
    private Integer sourceId;
    private Integer dbId;
    private Integer tableId;
    private String dbName;
    private String tableName;
    private String odpsProject;
    private String odpsTable;
    private TaskType type;
    private TaskStatus status;
    private boolean stopped;
    private boolean restart;
    private Integer retriedTimes = 0;
    private Date startTime;
    private Date endTime;
    private Date createTime;
    private Date updateTime;

    // 非数据库字段
    private String sourceName;
    private String jobName;

    public void setType(TaskType type) {
        this.type = type;
    }

    @JsonIgnore
    private List<Integer> partitions;

    @JsonIgnore
    public String getTaskName() {
        return String.format("%d.%d.%s.%s.%d", sourceId, jobId, dbName, tableName, id);
    }
}
