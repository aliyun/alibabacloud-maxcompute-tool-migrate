package com.aliyun.odps.mma.util;

import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import org.springframework.stereotype.Component;

@Component
public class TableSchemaHandler extends JsonHandler<MMATableSchema> {
    TableSchemaHandler() {
        super();
        dataType = MMATableSchema.class;
    }
}
