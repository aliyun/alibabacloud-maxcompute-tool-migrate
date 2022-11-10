package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.type.TypeInfo;
import org.springframework.stereotype.Component;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.type.TypeInfoParser;

import java.util.Objects;

@Component
public class OdpsToOdpsSchemaAdapter implements OdpsSchemaAdapter {
    @Override
    public TableSchema toOdpsSchema(MMATableSchema mmaTableSchema) {
        TableSchema tableSchema = new TableSchema();
        mmaTableSchema.getColumns().forEach(columnSchema -> {
            tableSchema.addColumn(convertToOdpsColumn(columnSchema));
        });

        mmaTableSchema.getPartitions().forEach(partitionSchema -> {
            tableSchema.addPartitionColumn(convertToOdpsColumn(partitionSchema));
        });
        return tableSchema;
    }

    @Override
    public SourceType sourceType() {
        return SourceType.ODPS;
    }

    private Column convertToOdpsColumn(MMAColumnSchema columnSchema) {
        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(columnSchema.getType());

        String comment = columnSchema.getComment();
        if (Objects.nonNull(comment)) {
            comment = comment.replace("'", "\\'");
        }

        return new Column(columnSchema.getName(), typeInfo, comment);
    }

}
