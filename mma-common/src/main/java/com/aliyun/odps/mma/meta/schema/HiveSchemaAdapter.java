package com.aliyun.odps.mma.meta.schema;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.TypeInfoParser;

import java.util.Arrays;
import java.util.Objects;

@Component
@Primary
public class HiveSchemaAdapter implements OdpsSchemaAdapter {
    String[] odpsPartitionTypes = new String[] {
            "STRING",
            "VARCHAR",
            "CHAR",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT"
    };

    @Override
    public TableSchema toOdpsSchema(MMATableSchema mmaTableSchema) {
        TableSchema tableSchema = new TableSchema();
        mmaTableSchema.getColumns().forEach(columnSchema -> {
            tableSchema.addColumn(convertToOdpsColumn(columnSchema));
        });

        mmaTableSchema.getPartitions().forEach(partitionSchema -> {
            tableSchema.addPartitionColumn(convertToOdpsPartitionColumn(partitionSchema));
        });
        return tableSchema;
    }

    @Override
    public SourceType sourceType() {
        return SourceType.HIVE;
    }

    private Column convertToOdpsColumn(MMAColumnSchema columnSchema) {
        // hive type string => odps TypeInfo
        // 1. all decimal(m,n) => decimal(36,18)
        // 2. all CHAR => STRING
        TypeInfo typeInfo = convertToOdpsType(columnSchema.getType());

        String comment = columnSchema.getComment();
        if (Objects.nonNull(comment)) {
            comment = comment.replace("'", "\\'");
        }

        return new Column(columnSchema.getName(), typeInfo, comment);
    }

    private Column convertToOdpsPartitionColumn(MMAColumnSchema columnSchema) {
        String type = columnSchema.getType().toUpperCase();
        TypeInfo odpsType = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING);
        if (Arrays.asList(odpsPartitionTypes).contains(type)) {
            odpsType = convertToOdpsType(type);
        }

        return new Column(columnSchema.getName(), odpsType, columnSchema.getComment());
    }

    private TypeInfo convertToOdpsType(String type) {
        // hive type string => odps TypeInfo
        // 1. all decimal(m,n) => decimal(36,18)
        // 2. all CHAR => STRING
        String hiveType = type.toUpperCase();

        // hiveType = hiveType.replaceAll("(?<=DECIMAL[^(]{0,10}\\()[^)]*", "36,18");
        hiveType = hiveType.replaceAll("(?<!VAR)CHAR.*\\)", "STRING");

        return TypeInfoParser.getTypeInfoFromTypeString(hiveType);
    }
}
