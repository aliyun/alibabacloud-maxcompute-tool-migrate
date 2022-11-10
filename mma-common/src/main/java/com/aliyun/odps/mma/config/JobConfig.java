package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.task.CommonPartitionGrouping;
import com.aliyun.odps.mma.task.HivePartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;
import com.aliyun.odps.mma.util.TableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import lombok.Data;

import java.util.*;
import java.util.stream.Collectors;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobConfig {
    @JsonProperty("partitions")
    List<Integer> partitions;
    @JsonProperty("tables")
    List<String> tables;
    @JsonProperty("task_type")
    TaskType taskType;
    @JsonProperty("table_blacklist")
    List<String> tableBlackList;
    @JsonProperty("table_whitelist")
    List<String> tableWhiteList;
    @JsonProperty("partition_filters")
    Map<String, String> partitionFilters;
    @JsonProperty("schema_only")
    Boolean schemaOnly;
    @JsonProperty("table_mapping")
    Map<String, String> tableMapping;
    @JsonProperty("increment")
    boolean increment = true;
    @JsonProperty("enable_verification")
    boolean enableVerification = true;
    Map<String, Object> others = new HashMap<>();

    @JsonIgnore
    MMAConfig mmaConfig;

    @JsonIgnore
    SourceConfig sourceConfig;

    @JsonAnyGetter
    public Map<String, Object> getOthers() {
        return others;
    }

    @JsonAnySetter
    public void setOthers(String key, Object value) {
        if (JobModel.JobModelInfo.isJobModelField(key)) {
            return;
        }

        this.others.put(key, value);
    }

    public Object getExtraConfig(String key) {
        if (Objects.nonNull(this.others)) {
            Object value = this.others.get(key);
            if (Objects.nonNull(value)) {
                return value;
            }
        }

        if (Objects.nonNull(this.sourceConfig)) {
            return this.sourceConfig.getConfig(key);
        }

        return null;
    }

    public String getString(String key) {
        Object value = getExtraConfig(key);
        if (Objects.nonNull(value)) {
            return (String) value;
        }
        return null;
    }

    public String getString(String key, String defaultValue) {
        String value = getString(key);
        if (Objects.nonNull(value)) {
            return value;
        }

        return defaultValue;
    }

    public Integer getInteger(String key) {
        Object value = getExtraConfig(key);
        if (Objects.nonNull(value)) {
            return (Integer) value;
        }

        return null;
    }

    public List<String> getList(String key) {
        Object value = getExtraConfig(key);
        if (Objects.nonNull(value)) {
            return (List<String>) value;
        }

        return null;
    }

    public TaskType getTaskType() {
        if (Objects.nonNull(taskType)) {
            return taskType;
        }

        return sourceConfig.defaultTaskType();
    }

    public TableName getDstOdpsTable(String srcTable, String dstOdpsProject) {
        if (Objects.isNull(dstOdpsProject)) {
            dstOdpsProject = mmaConfig.getConfig(MMAConfig.MC_DEFAULT_PROJECT);
        }

        if (Objects.nonNull(this.tableMapping) && this.tableMapping.containsKey(srcTable)) {
            String dstTable = this.tableMapping.get(srcTable);
            return new TableName(dstOdpsProject, dstTable);
        }
        return new TableName(dstOdpsProject, srcTable);
    }

    @JsonIgnore
    public Optional<PartitionFilter> getPartitionFilter(String tableName) {
        if (Objects.nonNull(partitionFilters) && partitionFilters.size() > 0) {
            String filterExpr = partitionFilters.get(tableName);
            if (Objects.nonNull(filterExpr)) {
                return Optional.of(new PartitionFilter(filterExpr));
            }
        }

        return Optional.empty();
    }

    @JsonIgnore
    public List<PartitionFilter> getPartitionFilters() {
        if (Objects.nonNull(partitionFilters) && partitionFilters.size() > 0) {
            return partitionFilters.values().stream().map(PartitionFilter::new).collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

    @JsonIgnore
    public Map<String, String> getHiveSettings() {
        SourceConfig config = this.sourceConfig;

        assert config instanceof HiveConfig;
        HiveConfig hiveConfig = (HiveConfig) config;
        Map<String, String> defaultSettings = hiveConfig.getMap(HiveConfig.HIVE_MR_JOB_SETTINGS);

        if (Objects.nonNull(others)) {
            Object o = others.get("hive_settings");

            if (Objects.nonNull(o)) {
                Map<String, String> jobSettings = (Map<String, String>) o;

                for (String key : jobSettings.keySet()) {
                    String value = jobSettings.get(key);

                    defaultSettings.put(key, value);
                }
            }
        }

        return defaultSettings;
    }

    @JsonIgnore
    public Boolean getSchemaOnly() {
        return schemaOnly != null && schemaOnly;
    }

    @JsonIgnore
    public PartitionGrouping getPartitionGrouping() {
        switch (this.getSourceConfig().getSourceType()) {
            case HIVE:
                if (Objects.nonNull(others)) {
                    Object maxNumObj = this.others.get("hive.task.partition.max.num");
                    Object maxSizeObj = this.others.get("hive.task.partition.max.size");

                    HiveConfig hiveConfig = (HiveConfig) this.sourceConfig;
                    int maxNum = hiveConfig.getInteger(HiveConfig.HIVE_TASK_PARTITION_MAX_NUM);
                    int maxSize = hiveConfig.getInteger(HiveConfig.HIVE_TASK_PARTITION_MAX_SIZE);

                    if (maxNumObj instanceof Integer) {
                        maxNum = (int)maxNumObj;
                    }

                    if (maxSizeObj instanceof Integer) {
                       maxSize = (int)maxSizeObj;
                    }

                    return new HivePartitionGrouping(maxNum, maxSize);
                }
            case ODPS:
                if (TaskType.MC2MC_VERIFY == this.taskType || TaskType.ODPS_INSERT_OVERWRITE == this.taskType) {
                    Object maxNumObj = this.others.get("task.partition.max.num");

                    OdpsConfig odpsConfig = (OdpsConfig) this.sourceConfig;
                    int maxNum = odpsConfig.getInteger(OdpsConfig.MC_TASK_PARTITION_MAX_NUM);

                    if (maxNumObj instanceof Integer) {
                        maxNum = (int)maxNumObj;
                    }

                    return new CommonPartitionGrouping(maxNum, -1);
                }
            default:
                break;
        }
        return this.sourceConfig.getPartitionGrouping();
    }
}
