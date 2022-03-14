package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract public class BaseFieldAdapter {
    protected Schema arrowSchema;
    protected Map<String, Field> fieldsMap = new HashMap<>();

    public BaseFieldAdapter(List<Column> columns, List<Column> partitionColumns) {
        List<Field> fields = new ArrayList<>(columns.size() + partitionColumns.size());

        for (Column column: columns) {
            Field field = OdpsToArrowTypeConverter.columnToArrowField(column);
            fields.add(field);
            fieldsMap.put(column.getName(), field);
        }

        for (Column column: partitionColumns) {
            Field field = OdpsToArrowTypeConverter.columnToArrowField(column);
            fields.add(field);
            fieldsMap.put(column.getName(), field);
        }

        arrowSchema = new Schema(fields);
    }

    abstract public FieldVector convert(Object... args);

    public Schema getArrowSchema() {
        return this.arrowSchema;
    }
}
