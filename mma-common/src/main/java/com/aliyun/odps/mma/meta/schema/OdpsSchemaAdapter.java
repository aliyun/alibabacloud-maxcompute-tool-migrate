package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.mma.constant.SourceType;

public interface OdpsSchemaAdapter {
    SourceType sourceType();
    com.aliyun.odps.TableSchema toOdpsSchema(MMATableSchema tableSchema);
}
