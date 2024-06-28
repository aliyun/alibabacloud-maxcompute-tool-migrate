package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.type.TypeInfo;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface OdpsSchemaAdapter {
    SourceType sourceType();

    default void checkCompatibility(MMATableSchema tableSchema) throws SchemaAdapterError {
        toOdpsSchema(tableSchema, -1, null);
    }

    default MMAOdpsTableSchema toOdpsSchema(
            MMATableSchema mmaTableSchema,
            int maxPtLevel,
            Map<String, String> columnMapping
    ) {
        return toOdpsSchema(mmaTableSchema, maxPtLevel, columnMapping, false);
    }

    default MMAOdpsTableSchema toOdpsSchema(
            MMATableSchema mmaTableSchema,
            int maxPtLevel,
            Map<String, String> columnMapping,
            boolean enableTS2
    ) {
        MMAOdpsTableSchema tableSchema = new MMAOdpsTableSchema();
        mmaTableSchema.getColumns().forEach(columnSchema -> {
            tableSchema.addColumn(convertToOdpsColumn(columnSchema, columnMapping));
        });

        List<MMAColumnSchema> ptColumns = mmaTableSchema.getPartitions();
        // 有合并分区配置时，将最后的几个分区转换为普通列
        if (maxPtLevel >= 0 && ptColumns.size() > maxPtLevel) {
            ptColumns.subList(maxPtLevel, ptColumns.size()).forEach(c -> {
                tableSchema.addColumn(convertToOdpsColumn(c, columnMapping));
            });

            ptColumns.subList(0, maxPtLevel).forEach(c -> {
                tableSchema.addPartitionColumn(convertToOdpsPartitionColumn(c, columnMapping));
            });
        } else {
            ptColumns.forEach(partitionSchema -> {
                tableSchema.addPartitionColumn(convertToOdpsPartitionColumn(partitionSchema, columnMapping));
            });
        }

        if (ListUtils.size(mmaTableSchema.getPrimaryKeys()) > 0) {
            tableSchema.setPrimaryKeys(mmaTableSchema.getPrimaryKeys());
            List<String> primaryKeys = mmaTableSchema.getPrimaryKeys();

            tableSchema.getColumns().forEach(c -> {
                if (primaryKeys.contains(c.getName())) {
                    c.setNullable(false);
                }
            });
        }

        tableSchema.setEnableTransaction(enableTS2);

//        if (Objects.nonNull(tableSchema.getEnableTransaction()) && tableSchema.getEnableTransaction()) {
//            if (ListUtils.size(mmaTableSchema.getPrimaryKeys()) == 0) {
//                throw new SchemaAdapterError("table with transaction enabled must have primary keys, table is" + mmaTableSchema.getName());
//            }
//        }

        return tableSchema;
    }

    default Column convertToOdpsColumn(MMAColumnSchema mmaColumnSchema, Map<String, String> columnMappings) {
        TypeInfo odpsType = convertToOdpsType(mmaColumnSchema);
        return convertToOdpsColumn(mmaColumnSchema, odpsType, columnMappings);
    }

    default Column convertToOdpsColumn(
            MMAColumnSchema mmaColumnSchema,
            TypeInfo odpsType,
            Map<String, String> columnMapping
    ) {
        String comment = mmaColumnSchema.getComment();
        if (Objects.nonNull(comment)) {
            comment = comment.replace("'", "\\'");
        }

        String srcColumnName =  mmaColumnSchema.getName();
        String odpsColumnName;
        if (Objects.nonNull(columnMapping)) {
            odpsColumnName = columnMapping.getOrDefault(srcColumnName, srcColumnName);
        } else {
            odpsColumnName = srcColumnName;
        }

        return new Column(odpsColumnName, odpsType, comment);
    }


    default Column convertToOdpsPartitionColumn(MMAColumnSchema mmaColumnSchema, Map<String, String> columnMapping) {
        TypeInfo odpsType = convertToOdpsPartitionType(mmaColumnSchema);
        return convertToOdpsColumn(mmaColumnSchema, odpsType, columnMapping);
    }

    TypeInfo convertToOdpsType(MMAColumnSchema columnSchema);
    TypeInfo convertToOdpsPartitionType(MMAColumnSchema columnSchema);
}
