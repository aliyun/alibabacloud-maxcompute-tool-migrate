package com.aliyun.odps.mma.meta.schema;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.TypeInfoParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    String[] specialChars = new String[] {"#", "$"};

    @Override
    public SourceType sourceType() {
        return SourceType.HIVE;
    }

    @Override
    public TypeInfo convertToOdpsType(MMAColumnSchema columnSchema, JobConfig jobConfig) {
        // hive type string => odps TypeInfo
        // 1. all decimal(m,n) => decimal(36,18)
        // 2. all CHAR => STRING
        return convertToOdpsType(columnSchema.getType());
    }

    @Override
    public TypeInfo convertToOdpsPartitionType(MMAColumnSchema columnSchema) {
        String type = columnSchema.getType().toUpperCase();
        TypeInfo odpsType = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING);
        if (Arrays.asList(odpsPartitionTypes).contains(type)) {
            odpsType = convertToOdpsType(type);
        }

        return odpsType;
    }

    private TypeInfo convertToOdpsType(String type) {
        // hive type string => odps TypeInfo
        // all CHAR => STRING
        String hiveType = type.toUpperCase();

        // hiveType = hiveType.replaceAll("(?<=DECIMAL[^(]{0,10}\\()[^)]*", "36,18");
        hiveType = hiveType.replaceAll("(?<!VAR)CHAR.*\\)", "STRING");

        return TypeInfoParser.getTypeInfoFromTypeString(hiveType);
    }

    private String trimSpecialChar(String columnName) {
        // 这里假设列名只会以一个特殊字符开头
        for (String specialChar: specialChars) {
            if (columnName.startsWith(specialChar) && columnName.length() > 1) {
                columnName = "alias_" + columnName.substring(1);
                break;
            }
        }

        return columnName;
    }

    public static void main(String[] args) {
        List<Integer> x = new ArrayList<>();
        x.add(1);
        x.add(2);

        System.out.println(x.subList(0, 0).size());
    }
}
