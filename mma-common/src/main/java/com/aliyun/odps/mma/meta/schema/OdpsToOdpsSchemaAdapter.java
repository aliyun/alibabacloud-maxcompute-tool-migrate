package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.type.TypeInfo;
import org.springframework.stereotype.Component;


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

    @Override
    public DstOdpsTableSchema toOdpsSchema(TableModel tableModel, TaskProxy taskProxy) {
        DstOdpsTableSchema odpsTableSchema = OdpsSchemaAdapter.super.toOdpsSchema(tableModel, taskProxy);

        OdpsConfig odpsConfig = (OdpsConfig) taskProxy.getJobConfig().getSourceConfig();

        int maxLifeCycle = odpsConfig.getTableMaxLifeCycle();
        Integer sourceLifeCycle = odpsTableSchema.getLifeCycle();

        if (Objects.nonNull(sourceLifeCycle) && sourceLifeCycle > maxLifeCycle) {
            odpsTableSchema.setLifeCycle(maxLifeCycle);
        }

        return odpsTableSchema;
    }
}
