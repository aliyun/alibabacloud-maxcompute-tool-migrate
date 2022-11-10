package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.mma.constant.SourceType;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class HiveOssSchemaAdapter extends HiveSchemaAdapter {
    @Override
    public SourceType sourceType() {
        return SourceType.HIVE_OSS;
    }
}
