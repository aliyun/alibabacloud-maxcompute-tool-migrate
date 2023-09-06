package com.aliyun.odps.mma.meta.schema;

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
    public Column convertToOdpsColumn(MMAColumnSchema columnSchema) {
        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(columnSchema.getType());

        String comment = columnSchema.getComment();
        if (Objects.nonNull(comment)) {
            comment = comment.replace("'", "\\'");
        }

        return new Column(columnSchema.getName(), typeInfo, comment);
    }

    @Override
    public Column convertToOdpsPartitionColumn(MMAColumnSchema columnSchema) {
        return convertToOdpsColumn(columnSchema);
    }
}
