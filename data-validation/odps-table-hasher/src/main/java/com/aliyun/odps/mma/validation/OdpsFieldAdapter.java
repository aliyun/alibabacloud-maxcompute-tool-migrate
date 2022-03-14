package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Collections;
import java.util.List;

public class OdpsFieldAdapter extends BaseFieldAdapter {
    public OdpsFieldAdapter(List<Column> columns) {
        super(columns, Collections.<Column>emptyList());
    }

    @Override
    public FieldVector convert(Object... args) {
        String filedName = (String) args[0];
        Object fieldValue = args[1];

        Field field = fieldsMap.get(filedName);
        BufferAllocator bufferAllocator = new RootAllocator();
        FieldVector fv = field.createVector(bufferAllocator);
        fv.setInitialCapacity(1);
        fv.allocateNew();

        FieldWriter fieldWriter = fv.getMinorType().getNewFieldWriter(fv);
        OdpsArrowWriter.write(fieldWriter, fieldValue, fv);
        fv.setValueCount(1);

        return fv;
    }
}
