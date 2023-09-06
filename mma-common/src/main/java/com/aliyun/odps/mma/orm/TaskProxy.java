package com.aliyun.odps.mma.orm;

import java.util.*;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import com.aliyun.odps.mma.meta.schema.OdpsSchemaAdapter;
import com.aliyun.odps.mma.meta.schema.SchemaUtils;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TaskLog;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.service.PartitionService;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.sql.PartitionValue;
import com.aliyun.odps.mma.util.ExceptionUtils;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TaskProxy {
    private final TaskService taskService;
    private final PartitionService partitionService;
    private final SchemaUtils schemaUtils;

    @Getter
    private TaskModel taskModel;
    @Getter
    private JobModel jobModel;
    @Getter
    private JobConfig jobConfig;
    @Getter
    private TableProxy table;

    // partition缓存，不能直接用，需要使用getPartitions方法获取。
    private List<PartitionModel> _partitions;

    public void init(TaskModel taskModel, JobModel jobModel, JobConfig jobConfig, TableProxy table) {
        this.taskModel = taskModel;
        this.jobModel = jobModel;
        this.jobConfig = jobConfig;
        this.table = table;
    }

    @Autowired
    protected TaskProxy(TaskService taskService, PartitionService partitionService, SchemaUtils schemaUtils) {
        this.taskService = taskService;
        this.partitionService = partitionService;
        this.schemaUtils = schemaUtils;
    }

    public void setStatus(TaskStatus status) {
        this.taskModel.setStatus(status);
        this.taskService.updateTaskStatus(this.taskModel);
    }

    public void setSubStatus(String status) {
        this.taskModel.setSubStatus(status);
        this.taskService.updateTaskSubStatus(this.taskModel);
    }

    public void error(String action, Exception e) {
        error(action, ExceptionUtils.getStackTrace(e));
    }

    public void error(String action, String msg) {
        setStatus(TaskStatus.getFailStatus(getStatus()));
        log(action, msg);
    }

    public void setTaskStart() {
        this.taskService.setTaskStart(taskModel.getId());
    }

    public void setTaskEnd() {
        this.taskModel.setStatus(TaskStatus.DONE);
        this.taskService.setTaskEnd(this.taskModel.getId());
    }

    public void log(String action, String msg) {
        TaskLog taskLog = new TaskLog();
        taskLog.setTaskId(this.taskModel.getId());
        taskLog.setStatus(this.taskModel.getStatus());
        taskLog.setAction(action);
        taskLog.setMsg(msg);
        this.taskService.addTaskLog(taskLog);
    }

    public boolean isRestarted() {
        return this.taskModel.isRestart();
    }

    public boolean requireMergePartition() {
        Integer maxPartitionLevel = jobConfig.getMaxPartitionLevel();

        return Objects.nonNull(maxPartitionLevel) && maxPartitionLevel >= 0;
    }

    public int getMaxPartitionLevel() {
        Integer maxPartitionLevel = jobConfig.getMaxPartitionLevel();

        if (Objects.isNull(maxPartitionLevel)) {
            return -1;
        }

        return maxPartitionLevel;
    }

    public void resetRestart() {
        taskService.resetRestart(taskModel.getId());
    }

    public TaskStatus getStatus() {
        return this.taskModel.getStatus();
    }

    public String getSubStatus() {
        String status = this.taskModel.getSubStatus();
        if (Objects.isNull(status)) {
            return "";
        }

        return status;
    }

    public int getId() {
        return this.taskModel.getId();
    }

    public String getTaskName() {
        return taskModel.getTaskName();
    }

    public int getTableId() {
        return this.taskModel.getTableId();
    }

    public int getDbId() {
        return this.taskModel.getDbId();
    }

    public String getDbName() {
        return this.taskModel.getDbName();
    }

    public String getTableName() {
        return this.taskModel.getTableName();
    }

    public String getTableFullName() {
        return String.format("%s.%s", taskModel.getDbName(), taskModel.getTableName());
    }

    public String getOdpsProjectName() {
        return this.taskModel.getOdpsProject();
    }

    public String getOdpsTableName() {
        return this.taskModel.getOdpsTable();
    }

    public String getOdpsTableFullName() {
        return String.format("%s.`%s`", getOdpsProjectName(), getOdpsTableName());
    }

    public TableSchema getOdpsTableSchema(boolean sameAsSrc) {
        if (sameAsSrc) {
            return getOdpsTableSchema();
        }

        OdpsSchemaAdapter schemaAdapter = schemaUtils.getSchemaAdapter(jobConfig.getSourceConfig().getSourceType());
        return schemaAdapter.toOdpsSchema(this.table.getTableSchema(), -1);
    }

    public TableSchema getOdpsTableSchema() {
        OdpsSchemaAdapter schemaAdapter = schemaUtils.getSchemaAdapter(jobConfig.getSourceConfig().getSourceType());
        return schemaAdapter.toOdpsSchema(this.table.getTableSchema(), jobConfig.getMaxPartitionLevel());
    }

    public int getPartitionNum() {
        return taskService.getPartitionNumOfTask(taskModel.getId());
    }

    public List<PartitionModel> getPartitions() {
        if (Objects.isNull(_partitions)) {
            _partitions = partitionService.getPartitionsOfTask(taskModel.getId());
        }

        return _partitions;
    }

    /**
     * 获取 odps端partition value, partition value的type是经过转换后的odps type名称
     */
//    public List<PartitionValue> getOdpsPartitionValues() {
//        List<Column> partitionColumns = getOdpsTableSchema().getPartitionColumns();
//        List<MMAColumnSchema> mmaColumns = partitionColumns
//                .stream()
//                .map(MMAColumnSchema::fromOdpsColumn)
//                .collect(Collectors.toList());
//
//        List<PartitionModel> partitionModels = this.getPartitions();
//
//        return partitionModels
//                .stream()
//                .map(pm -> new PartitionValue(mmaColumns, pm.getValue()))
//                .collect(Collectors.toList());
//    }

    /**
     * 获取 原始partition value
     */
    public List<PartitionValue> getSrcPartitionValues() {
//        List<MMAColumnSchema> columns = table.getTableSchema().getPartitions();
//        List<PartitionModel> partitionModels = this.getPartitions();
//
//        return partitionModels
//                .stream()
//                .map(pm -> new PartitionValue(columns, pm.getValue()))
//                .collect(Collectors.toList());
        return getDstOdpsPartitionValues();
    }

    public List<PartitionValue> getDstOdpsPartitionValues() {
        List<Column> partitionColumns = getOdpsTableSchema().getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            return Collections.emptyList();
        }

        List<MMAColumnSchema> mmaColumns = partitionColumns
                .stream()
                .map(MMAColumnSchema::fromOdpsColumn)
                .collect(Collectors.toList());

        List<PartitionModel> partitionModels = this.getPartitions();

        int maxPartitionLevel = getMaxPartitionLevel();
        // 等于0时，相当于把分区表转换为分分区表，会在上面if语句的时候就返回，不会走到这里
        if (maxPartitionLevel > 0) {
            Set<String> valueSet = new HashSet<>();


            return partitionModels
                    .stream()
                    .map(pm -> {
                        String[] keyValues = pm.getValue().split("/");
                        String value = "";

                        for (int i = 0; i < maxPartitionLevel; i ++) {
                            value += keyValues[i];

                            if (i < maxPartitionLevel - 1) {
                                value += "/";
                            }
                        }

                        if (valueSet.contains(value)) {
                            return null;
                        }

                        valueSet.add(value);

                        return new PartitionValue(mmaColumns, value);
                    }).filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } else {
            return partitionModels
                    .stream()
                    .map(pm -> {
                        return new PartitionValue(mmaColumns, pm.getValue());
                    })
                    .collect(Collectors.toList());
        }
    }


}
