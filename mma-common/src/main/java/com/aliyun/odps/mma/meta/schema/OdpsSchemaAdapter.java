package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.StorageTierInfo;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.mma.util.StringUtils;
import com.aliyun.odps.type.TypeInfo;

import java.util.List;
import java.util.Map;
import java.util.Objects;


public interface OdpsSchemaAdapter {
    SourceType sourceType();

    default void checkCompatibility(TableModel tableModel, TaskProxy taskProxy) throws SchemaAdapterError {
        toOdpsSchema(tableModel, taskProxy);
    }

    default DstOdpsTableSchema toOdpsSchema(TableModel tableModel, TaskProxy taskProxy) {
        JobConfig jobConfig = taskProxy.getJobConfig();

        String tableId = tableModel.getFullName();
        MMATableSchema srcTableSchema = tableModel.getSchema();

        Map<String, String> columnMapping = jobConfig.getColumnMapping();
        DstOdpsTableSchema dstOdpsTableSchema = new DstOdpsTableSchema();
        dstOdpsTableSchema.setProjectName(taskProxy.getOdpsProjectName());
        dstOdpsTableSchema.setSchemaName(taskProxy.getOdpsSchemaName());
        dstOdpsTableSchema.setTableName(taskProxy.getOdpsTableName());

        srcTableSchema.getColumns().forEach(columnSchema -> {
            dstOdpsTableSchema.addColumn(convertToOdpsColumn(columnSchema, columnMapping, jobConfig));
        });

        List<MMAColumnSchema> ptColumns = srcTableSchema.getPartitions();

        if (Objects.nonNull(ptColumns)) {
            // 有合并分区配置时，将最后的几个分区转换为普通列
            int maxPtLevel = jobConfig.getMaxPartitionLevel();
            if (maxPtLevel >= 0 && ptColumns.size() > maxPtLevel) {
                ptColumns.subList(maxPtLevel, ptColumns.size()).forEach(c -> {
                    dstOdpsTableSchema.addColumn(convertToOdpsColumn(c, columnMapping, jobConfig));
                });

                ptColumns.subList(0, maxPtLevel).forEach(c -> {
                    dstOdpsTableSchema.addPartitionColumn(convertToOdpsPartitionColumn(c, columnMapping));
                });
            } else {
                ptColumns.forEach(partitionSchema -> {
                    dstOdpsTableSchema.addPartitionColumn(convertToOdpsPartitionColumn(partitionSchema, columnMapping));
                });
            }
        }

        // priority: user config > src delta > original
        String tableType = "";

        if (Objects.nonNull(jobConfig.getTableType())) {
            tableType = jobConfig.getTableType();
        }

        Map tableConfigMap = (Map) jobConfig.getOthers().get("mc.table.type");
        if (tableConfigMap != null && tableConfigMap.get(tableId) != null) {
            tableType = (String) ((Map)tableConfigMap.get(tableId)).get("table.type");
        }

        if (ListUtils.size(srcTableSchema.getPrimaryKeys()) > 0 && StringUtils.isBlank(tableType)) {
            tableType = "DELTA";
        }

        if (StringUtils.isBlank(tableType)) {
            tableType = "COMMON";
        }

        dstOdpsTableSchema.setTableType(DstOdpsTableSchema.DstOdpsTableType.fromValue(tableType));

        if (dstOdpsTableSchema.isAcid1() || dstOdpsTableSchema.isAcid2()) {
            dstOdpsTableSchema.setEnableTransaction(true);
        }

        if (dstOdpsTableSchema.isAcid2()) {
            List<String> configPk = null;
            if (tableConfigMap != null && tableConfigMap.get(tableId) != null) {
                configPk = (List<String>) ((Map)tableConfigMap.get(tableId)).get("pk");
            }

            if (ListUtils.size(configPk) > 0) {
                dstOdpsTableSchema.setPrimaryKeys(configPk);
            } else if (ListUtils.size(srcTableSchema.getPrimaryKeys()) > 0) {
                dstOdpsTableSchema.setPrimaryKeys(srcTableSchema.getPrimaryKeys());
                List<String> primaryKeys = srcTableSchema.getPrimaryKeys();

                dstOdpsTableSchema.getColumns().forEach(c -> {
                    if (primaryKeys.contains(c.getName())) {
                        c.setNullable(false);
                    }
                });
            } else {
                throw new IllegalArgumentException("delta table must have primary keys, table is " + tableId);
            }
        }

        if (srcTableSchema.getClusterInfo() != null) {
            dstOdpsTableSchema.setClusterInfo(srcTableSchema.getClusterInfo());
        }

        dstOdpsTableSchema.setDisableLifeCycle(srcTableSchema.isDisableLifeCycle());
        dstOdpsTableSchema.setTblProperties(srcTableSchema.getTblProperties());

        if (srcTableSchema.getTblProperties() != null) {
//            if (srcTableSchema.getTblProperties().containsKey("DisableLifeCycle")) {
//                dstOdpsTableSchema.setDisableLifeCycle(Boolean.parseBoolean(srcTableSchema.getTblProperties().get("DisableLifeCycle")));
//            }

            if (srcTableSchema.getTblProperties().containsKey("lifecycleConfig")) {
                dstOdpsTableSchema.setStorageTierLifeCycleConfigJson(srcTableSchema.getTblProperties().get("lifecycleConfig"));
            }

            if (srcTableSchema.getTblProperties().containsKey("storageTierInfo")) {
                try {
                    dstOdpsTableSchema.setStorageTier(StorageTierInfo.StorageTier.getStorageTierByName(srcTableSchema.getTblProperties().get("storageTierInfo")));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("storageTierInfo is invalid, table is " + tableId + " tierinfo: " + srcTableSchema.getTblProperties().get("storageTierInfo"));
                }
            }
        }

        dstOdpsTableSchema.setComment(srcTableSchema.getComment());
        dstOdpsTableSchema.setLifeCycle(tableModel.getLifecycle());

        return dstOdpsTableSchema;
    }

    default Column convertToOdpsColumn(MMAColumnSchema mmaColumnSchema, Map<String, String> columnMappings, JobConfig jobConfig) {
        TypeInfo odpsType = convertToOdpsType(mmaColumnSchema, jobConfig);
        return convertToOdpsColumn(mmaColumnSchema, odpsType, columnMappings);
    }

    default Column convertToOdpsColumn(
            MMAColumnSchema mmaColumnSchema,
            TypeInfo odpsType,
            Map<String, String> columnMapping
    ) {
        String comment = mmaColumnSchema.getComment();
        if (Objects.nonNull(comment)) {
            comment = comment.replace("\\", "\\\\").replace("'", "\\'");
        }

        String srcColumnName =  mmaColumnSchema.getName();
        String odpsColumnName;
        if (Objects.nonNull(columnMapping)) {
            odpsColumnName = columnMapping.getOrDefault(srcColumnName, srcColumnName);
        } else {
            odpsColumnName = srcColumnName;
        }

        Column odpsColumn = new Column(odpsColumnName, odpsType, comment);

        if (Objects.nonNull(mmaColumnSchema.getNullable())) {
            odpsColumn.setNullable(mmaColumnSchema.getNullable());
        }

        //odpsColumn.setDefaultValue(mmaColumnSchema.getDefaultValue());

        return odpsColumn;
    }


    default Column convertToOdpsPartitionColumn(MMAColumnSchema mmaColumnSchema, Map<String, String> columnMapping) {
        TypeInfo odpsType = convertToOdpsPartitionType(mmaColumnSchema);
        return convertToOdpsColumn(mmaColumnSchema, odpsType, columnMapping);
    }

    TypeInfo convertToOdpsType(MMAColumnSchema columnSchema, JobConfig jobConfig);
    TypeInfo convertToOdpsPartitionType(MMAColumnSchema columnSchema);
}
