package com.aliyun.odps.mma.meta.schema;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import com.aliyun.odps.mma.constant.SourceType;

@Component
@Primary
public class GlueSchemaAdapter extends HiveSchemaAdapter {
    @Override
    public SourceType sourceType() {
        return SourceType.HIVE_GLUE;
    }
}
