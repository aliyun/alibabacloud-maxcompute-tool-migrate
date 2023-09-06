package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.constant.SourceType;

import java.util.List;

public interface OdpsSchemaAdapter {
    SourceType sourceType();

    default com.aliyun.odps.TableSchema toOdpsSchema(MMATableSchema tableSchema) {
        return toOdpsSchema(tableSchema, -1);
    }

    default com.aliyun.odps.TableSchema toOdpsSchema(MMATableSchema mmaTableSchema, int maxPtLevel) {
        TableSchema tableSchema = new TableSchema();

        mmaTableSchema.getColumns().forEach(columnSchema -> {
            tableSchema.addColumn(convertToOdpsColumn(columnSchema));
        });

        List<MMAColumnSchema> ptColumns = mmaTableSchema.getPartitions();
        // 有合并分区配置时，将最后的几个分区转换为普通列
        if (maxPtLevel >= 0 && ptColumns.size() > maxPtLevel) {
            ptColumns.subList(maxPtLevel, ptColumns.size()).forEach(c -> {
                tableSchema.addColumn(convertToOdpsColumn(c));
            });

            ptColumns.subList(0, maxPtLevel).forEach(c -> {
                tableSchema.addPartitionColumn(convertToOdpsPartitionColumn(c));
            });
        } else {
            ptColumns.forEach(partitionSchema -> {
                tableSchema.addPartitionColumn(convertToOdpsPartitionColumn(partitionSchema));
            });
        }

        return tableSchema;
    }

    Column convertToOdpsColumn(MMAColumnSchema columnSchema);

    Column convertToOdpsPartitionColumn(MMAColumnSchema columnSchema);
}
