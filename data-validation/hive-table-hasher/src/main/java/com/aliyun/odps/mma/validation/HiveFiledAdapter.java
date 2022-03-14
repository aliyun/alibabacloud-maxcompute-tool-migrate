package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;
import com.aliyun.odps.mma.validation.HiveArrowWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.IOException;
import java.util.List;


public class HiveFiledAdapter extends BaseFieldAdapter {
    public HiveFiledAdapter(List<Column> columns, List<Column> partitionColumns) {
        super(columns, partitionColumns);
    }

    @Override
    public FieldVector convert(Object... args) {
        ObjectInspector hiveInspector = (ObjectInspector) args[0];
        Object hiveObject = args[1];
        Column column = (Column) args[2];

        // 1. get arrow Field according column name
        Field field = fieldsMap.get(column.getName());

        // 2. create FieldVector by Field
        BufferAllocator bufferAllocator = new RootAllocator();
        FieldVector fv = field.createVector(bufferAllocator);
        fv.setInitialCapacity(1);
        fv.allocateNew();

        // 3. write data to FieldVector through FieldWriter
        FieldWriter fieldWriter = fv.getMinorType().getNewFieldWriter(fv);
        HiveArrowWriter.write(fieldWriter, hiveInspector, hiveObject, column.getTypeInfo(), fv);

        fv.setValueCount(1);

        return fv;
    }

}