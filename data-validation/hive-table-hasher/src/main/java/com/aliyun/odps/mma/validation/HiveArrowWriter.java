package com.aliyun.odps.mma.validation;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class HiveArrowWriter {
    private static final Map<PrimitiveCategory, HiveFieldWriter> primitiveWriterMap = new HashMap<PrimitiveCategory, HiveFieldWriter>() {{
        put(PrimitiveCategory.BYTE, new ByteWriter());                     // tinyint
        put(PrimitiveCategory.SHORT, new ShortWriter());                   // smallint
        put(PrimitiveCategory.INT, new IntegerWriter());                   // int
        put(PrimitiveCategory.LONG, new LongWriter());                     // bigint
        put(PrimitiveCategory.FLOAT, new FloatWriter());                   // float -> bigint, 保留6位小数
        put(PrimitiveCategory.DOUBLE, new DoubleWriter());                 // double -> bigint, 保留6位小数
        put(PrimitiveCategory.DECIMAL, new BigDecimalWriter());            // decimal
        put(PrimitiveCategory.BOOLEAN, new BooleanWriter());               // boolean
        put(PrimitiveCategory.STRING, new StringWriter());                 // string
        put(PrimitiveCategory.CHAR, new CharWriter());                     // hive char -> odps string
        put(PrimitiveCategory.VARCHAR, new VarcharWriter());               // varchar
        put(PrimitiveCategory.BINARY, new BinaryWriter());                 // binary
        put(PrimitiveCategory.DATE, new DateWriter());                     // hive date -> odps datetime
        put(PrimitiveCategory.TIMESTAMP, new TimestampWriter());           // timestamp
     }};
    
    public static void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject, TypeInfo odpsTypeInfo, FieldVector fieldVector) {
        if (hiveObject == null) {
            fieldWriter.writeNull();
            return;
        }

        switch (inspector.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector pi = (PrimitiveObjectInspector) inspector;
                HiveFieldWriter writer = primitiveWriterMap.get(pi.getPrimitiveCategory());
                if (writer == null) {
                    throw new RuntimeException("unsupported hive type " + pi.getPrimitiveCategory());
                }
                writer.write(fieldWriter, inspector, hiveObject, odpsTypeInfo, fieldVector);
                break;
            }
            case LIST: {
                ListWriter writer = new ListWriter();
                writer.write(fieldWriter, inspector, hiveObject, odpsTypeInfo, fieldVector);
                break;
            }
            case MAP: {
                MapWriter writer = new MapWriter();
                writer.write(fieldWriter, inspector, hiveObject, odpsTypeInfo, fieldVector);
                break;
            }
            case STRUCT: {
                StructWriter writer = new StructWriter();
                writer.write(fieldWriter, inspector, hiveObject, odpsTypeInfo, fieldVector);
                break;
            }
        }
    }

    public static class HiveFieldWriter {
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {

        }

        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject, TypeInfo odpsTypeInfo, FieldVector fieldVector) {
            write(fieldWriter, inspector, hiveObject);
        }
    }
 
    static class ByteWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            Byte data = ((ByteObjectInspector) inspector).get(hiveObject);
            fieldWriter.writeTinyInt(data);
        }
    }

    static class ShortWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            short data = ((ShortObjectInspector)inspector).get(hiveObject);
            fieldWriter.writeSmallInt(data);
        }
    }

    static class IntegerWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            int data = ((IntObjectInspector) inspector).get(hiveObject);
            fieldWriter.writeInt(data);
        }
    }

    static class LongWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            long data = ((LongObjectInspector) inspector).get(hiveObject);
            fieldWriter.writeBigInt(data);
        }
    }

    static class FloatWriter extends HiveFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator();

        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            Float data = ((FloatObjectInspector) inspector).get(hiveObject);
            byte[] value = String.format("%.6f", data).getBytes();
            ArrowBuf buf = bufferAllocator.buffer(value.length);
            buf.setBytes(0, value);

            fieldWriter.writeVarChar(0, value.length, buf);
            buf.close();
        }
    }

    static class DoubleWriter extends HiveFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator();

        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            double data = ((DoubleObjectInspector) inspector).get(hiveObject);
            byte[] value = String.format("%.15f", data).getBytes();
            ArrowBuf buf = bufferAllocator.buffer(value.length);
            buf.setBytes(0, value);

            fieldWriter.writeVarChar(0, value.length, buf);
            buf.close();
        }
    }

    static class BigDecimalWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            BigDecimal data = ((HiveDecimalObjectInspector)inspector).getPrimitiveJavaObject(hiveObject).bigDecimalValue();
            data = data.setScale(18, RoundingMode.HALF_UP);
            fieldWriter.writeDecimal(data);
        }
    }

    static class BooleanWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            boolean data = ((BooleanObjectInspector) inspector).get(hiveObject);
            fieldWriter.writeBit(data ? 1 : 0);
        }
    }

    static class StringWriter extends HiveFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator();

        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            byte[] data = ((StringObjectInspector) inspector).getPrimitiveJavaObject(hiveObject).getBytes();
            ArrowBuf buf = bufferAllocator.buffer(data.length);
            buf.setBytes(0, data);

            fieldWriter.writeVarChar(0, data.length, buf);
            buf.close();
        }
    }

    static class CharWriter extends HiveFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator();

        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            String value = ((HiveCharObjectInspector)inspector).getPrimitiveJavaObject(hiveObject).getValue();
            byte[] data = value.getBytes();
            ArrowBuf buf = bufferAllocator.buffer(data.length);
            buf.setBytes(0, data);

            fieldWriter.writeVarChar(0, data.length, buf);
            buf.close();
        }
    }

    static class VarcharWriter extends HiveFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator();

        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            String value = ((HiveVarcharObjectInspector)inspector).getPrimitiveJavaObject(hiveObject).getValue();
            byte[] data = value.getBytes();
            ArrowBuf buf = bufferAllocator.buffer(data.length);
            buf.setBytes(0, data);

            fieldWriter.writeVarChar(0, data.length, buf);
            buf.close();
        }
    }

    static class BinaryWriter extends HiveFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator();

        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            byte[] data = ((BinaryObjectInspector) inspector).getPrimitiveJavaObject(hiveObject);
            ArrowBuf buf = bufferAllocator.buffer(data.length);
            buf.setBytes(0, data);

            fieldWriter.writeVarBinary(0, data.length, buf);
            buf.close();
        }
    }

    static class DateWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            //Object rawDate = ((DateObjectInspector)inspector).getPrimitiveJavaObject(hiveObject);
            Object rawDate;

            try {
                Class<?> dateInspectorClazz = Class.forName(
                        "org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector");
                Method getPrimitiveJavaObject = dateInspectorClazz
                        .getDeclaredMethod("getPrimitiveJavaObject", Object.class);
                rawDate = getPrimitiveJavaObject.invoke(dateInspectorClazz.cast(inspector), hiveObject);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            long dateTimeValue;

            switch (rawDate.getClass().getName()) {
                // for hive1, hive2
                case "java.sql.Date":
                    dateTimeValue = ((java.sql.Date) rawDate).getTime();
                    break;
                // for hive3
                case "org.apache.hadoop.hive.common.type.Date":
                    try {
                        Class<?> hiveDateClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
                        Method toEpochMilli = hiveDateClazz.getDeclaredMethod("toEpochMilli");
                        dateTimeValue = (long)toEpochMilli.invoke(rawDate);
                    }  catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                // unreachable
                default:
                    dateTimeValue = 0;
                    break;
            }

            fieldWriter.writeDateMilli(dateTimeValue);
        }
    }

    static class TimestampWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            //Object rawTs = ((TimestampObjectInspector) inspector).getPrimitiveJavaObject(hiveObject);
            Object rawTs;

            try {
                Class<?> timestampInspectorClazz = Class.forName(
                        "org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector");
                Method getPrimitiveJavaObject = timestampInspectorClazz
                        .getDeclaredMethod("getPrimitiveJavaObject", Object.class);
                rawTs = getPrimitiveJavaObject.invoke(timestampInspectorClazz.cast(inspector), hiveObject);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            long ts;
            switch (rawTs.getClass().getName()) {
                case "java.sql.Timestamp":
                    ts = ((java.sql.Timestamp) rawTs).getTime();
                    break;
                case "org.apache.hadoop.hive.common.type.Timestamp":
                    try {
                        Class.forName("org.apache.hadoop.hive.common.type.Timestamp");

                        Class<?> hiveTimestampClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
                        Method toEpochMilli = hiveTimestampClazz.getDeclaredMethod("toEpochMilli");
                        Method getNanos = hiveTimestampClazz.getDeclaredMethod("getNanos");
                        java.sql.Timestamp jt = new java.sql.Timestamp((Long) toEpochMilli.invoke(rawTs));
                        jt.setNanos((Integer) getNanos.invoke(rawTs));
                        ts = jt.getTime();
                    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                // unreachable
                default:
                    ts = 0;
                    break;
            }

            fieldWriter.writeTimeStampNano(ts);
        }
    }

    static class ListWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject) {
            ListObjectInspector li = (ListObjectInspector) inspector;
            ObjectInspector ei = li.getListElementObjectInspector();

            List<?> list = li.getList(hiveObject);
            fieldWriter.startList();
            for (Object el : list) {
                HiveArrowWriter.write(fieldWriter, ei, el, null, null);
            }
            fieldWriter.endList();
        }
    }

    static class MapWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter _ignore, ObjectInspector inspector, Object hiveObject, TypeInfo _odpsTypeInfo, FieldVector fieldVector) {
            MapObjectInspector mi = (MapObjectInspector) inspector;
            ObjectInspector keyInspector = mi.getMapKeyObjectInspector();
            ObjectInspector valueInspector = mi.getMapValueObjectInspector();

            UnionMapWriter writer = new UnionMapWriter((MapVector) fieldVector);
            writer.startMap();
            int count = 0;
            Map<?, ?> map = mi.getMap(hiveObject);
            for (Object key : map.keySet()) {
                writer.startEntry();
                Object value = map.get(key);
                HiveArrowWriter.write(writer.key(), keyInspector, key, null, fieldVector);
                HiveArrowWriter.write(writer.value(), valueInspector, value, null, fieldVector);
                writer.endEntry();

                count += 1;
            }
            writer.setValueCount(count);
            writer.endMap();
            writer.clear();
         }
    }

    static class StructWriter extends HiveFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, ObjectInspector inspector, Object hiveObject, TypeInfo odpsTypeInfo, FieldVector _fieldVector) {
            StructObjectInspector si = (StructObjectInspector) inspector;
            List<? extends StructField> hiveFields = si.getAllStructFieldRefs();

            StructTypeInfo odpsStructType = (StructTypeInfo) odpsTypeInfo;
            List<TypeInfo> odpsFields = odpsStructType.getFieldTypeInfos();

            fieldWriter.start();
            for (int i = 0, n = hiveFields.size(); i < n; i++) {
                StructField field = hiveFields.get(i);
                Object hiveFieldValue = si.getStructFieldData(hiveObject, field);
                TypeInfo odpsFieldType = odpsFields.get(i);
                FieldWriter structFieldWriter = StructFieldWriterGetter.getWriter(
                        fieldWriter,
                        field.getFieldName(),
                        odpsFieldType.getOdpsType()
                );

                HiveArrowWriter.write(structFieldWriter, field.getFieldObjectInspector(), hiveFieldValue, odpsFieldType, _fieldVector);
            }

            fieldWriter.end();
        }
    }
}
