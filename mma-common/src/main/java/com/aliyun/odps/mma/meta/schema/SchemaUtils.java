package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SchemaUtils {
    Map<SourceType, OdpsSchemaAdapter> schemaAdapterMap = new HashMap<>();

    @Autowired
    public SchemaUtils(List<OdpsSchemaAdapter> schemaAdapters) {
        for (OdpsSchemaAdapter schemaAdapter: schemaAdapters) {
            schemaAdapterMap.put(schemaAdapter.sourceType(), schemaAdapter);
        }
    }

    public OdpsSchemaAdapter getSchemaAdapter(SourceType sourceType) {
        return this.schemaAdapterMap.get(sourceType);
    }

    public OdpsSchemaAdapter getSchemaAdapter(TaskType taskType) {
        switch (taskType) {
            case HIVE:
            case HIVE_DATAX: return this.schemaAdapterMap.get(SourceType.HIVE);
            case MC2MC_VERIFY:
            case ODPS: return this.schemaAdapterMap.get(SourceType.ODPS);
            case OSS: return this.schemaAdapterMap.get(SourceType.OSS);
        }

        return null;
    }
}
