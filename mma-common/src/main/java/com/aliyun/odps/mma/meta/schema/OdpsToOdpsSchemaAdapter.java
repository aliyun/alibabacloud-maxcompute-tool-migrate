package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.mma.config.JobConfig;
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
    public SourceType sourceType() {
        return SourceType.ODPS;
    }

    @Override
    public TypeInfo convertToOdpsType(MMAColumnSchema columnSchema, JobConfig jobConfig) {
        return TypeInfoParser.getTypeInfoFromTypeString(columnSchema.getType());
     }

    @Override
    public TypeInfo convertToOdpsPartitionType(MMAColumnSchema columnSchema) {
        return convertToOdpsType(columnSchema, null);
    }
}
