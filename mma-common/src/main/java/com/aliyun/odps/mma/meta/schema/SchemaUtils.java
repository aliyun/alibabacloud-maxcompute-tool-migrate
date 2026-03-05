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


}
