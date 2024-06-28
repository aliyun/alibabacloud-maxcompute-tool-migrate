package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.TypeInfoParser;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
@Primary
public class DatabricksSchemaAdapter implements OdpsSchemaAdapter {
    List<String> unsupportedTypes = Arrays.asList(
            "INTERVAL",
            "TIMESTAMP_NTZ"
    );

    List<String> odpsPartitionTypes = Arrays.asList(
            "STRING",
            "VARCHAR",
            "CHAR",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT"
    );

    @Override
    public SourceType sourceType() {
        return SourceType.DATABRICKS;
    }

    @Override
    public void checkCompatibility(MMATableSchema tableSchema) throws SchemaAdapterError {
        if (Objects.nonNull(tableSchema.getForeignKeyConstrat())) {
            throw new SchemaAdapterError("maxcompute does not support foreign key, table is " + tableSchema.getName());
        }

        toOdpsSchema(tableSchema, -1, null);
    }

    @Override
    public TypeInfo convertToOdpsType(MMAColumnSchema columnSchema) {
        String columnType = columnSchema.getType().toUpperCase();

        if(unsupportedTypes.stream().anyMatch(columnType::startsWith)) {
            throw new SchemaAdapterError("maxcompute doesn't support type: " + columnType);
        }

        Map<String, String> extra = columnSchema.getExtra();
        if (Objects.nonNull(extra) && extra.containsKey("mask")) {
            throw new SchemaAdapterError("maxcomptue doesn't support column mask for column " + columnSchema.getName());
        }

        return TypeInfoParser.getTypeInfoFromTypeString(columnType);
    }

    @Override
    public TypeInfo convertToOdpsPartitionType(MMAColumnSchema columnSchema) {
        String type = columnSchema.getType().toUpperCase();
        if (odpsPartitionTypes.contains(type)) {
            return convertToOdpsType(type);
        }

        return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING);
    }

    private TypeInfo convertToOdpsType(String type) {
        // hive type string => odps TypeInfo
        // all CHAR => STRING
        String hiveType = type.toUpperCase();

        // hiveType = hiveType.replaceAll("(?<=DECIMAL[^(]{0,10}\\()[^)]*", "36,18");
        hiveType = hiveType.replaceAll("(?<!VAR)CHAR.*\\)", "STRING");

        return TypeInfoParser.getTypeInfoFromTypeString(hiveType);
    }
}
