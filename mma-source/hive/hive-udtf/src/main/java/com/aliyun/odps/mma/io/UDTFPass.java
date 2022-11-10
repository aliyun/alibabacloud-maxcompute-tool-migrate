package com.aliyun.odps.mma.io;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class UDTFPass extends GenericUDTF {
    private long count = 0;
    ObjectInspector[] objectInspectors;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        objectInspectors = args;
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("record_num");
        List<ObjectInspector> outputObjectInspectors = new ArrayList<>();
        outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                outputObjectInspectors);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        count += 1;

        if (count % 10000 == 0) {
            System.out.printf("has get %d records, %s\n", count, System.currentTimeMillis());
        }
    }

    @Override
    public void close() throws HiveException {
        forward(new Object[] {count});
    }
}
