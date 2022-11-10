package com.aliyun.odps.mma.meta;

import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class HiveMMASchemaAdapter {
    public static MMAColumnSchema fieldSchemaToColumn(FieldSchema fs) {
        return new MMAColumnSchema(
                fs.getName(),
                fs.getType(),
                fs.getComment()
        );
    }
}
