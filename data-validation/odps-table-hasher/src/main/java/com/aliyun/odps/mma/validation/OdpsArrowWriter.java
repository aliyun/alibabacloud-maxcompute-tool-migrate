package com.aliyun.odps.mma.validation;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import javafx.scene.SubScene;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class OdpsArrowWriter {
    private static Map<String, OdpsFieldWriter> writerMap = new HashMap<String, OdpsFieldWriter>() {{
        put("java.lang.Byte", new ByteWriter());                     // tinyint
        put("java.lang.Short", new ShortWriter());                   // smallint
        put("java.lang.Integer", new IntegerWriter());               // int
        put("java.lang.Long", new LongWriter());                     // bigint
        put("java.lang.Float", new FloatWriter());                   // float -> bigint, 保留6位小数
        put("java.lang.Double", new DoubleWriter());                 // double -> bigint, 保留6位小数
        put("java.math.BigDecimal", new BigDecimalWriter());         // decimal
        put("java.lang.Boolean", new BooleanWriter());               // boolean
        put("java.lang.String", new StringWriter());                 // string
        put("com.aliyun.odps.data.Varchar", new VarcharWriter());    // varchar
        put("com.aliyun.odps.data.Binary", new BinaryWriter());      // binary
        put("java.util.Date", new DateWriter());                     // datetime
        put("java.sql.Timestamp", new TimestampWriter());            // timestamp
        put("java.util.ArrayList", new ListWriter());                // array
        put("java.util.HashMap", new MapWriter());                   // map
        put("com.aliyun.odps.data.SimpleStruct", new StructWriter());      // struct
    }};

    public static void write(FieldWriter fieldWriter, Object odpsObject) {
        write(fieldWriter, odpsObject, null);
    }

    public static void write(FieldWriter fieldWriter, Object odpsObject, FieldVector fieldVector) {
        if (odpsObject == null) {
            fieldWriter.writeNull();
            return;
        }

        String className = odpsObject.getClass().getName();
        OdpsFieldWriter writer = writerMap.get(className);

        if (writer == null) {
            throw new RuntimeException("unsupported odps type " + className);
        }

        writer.write(fieldWriter, odpsObject, fieldVector);
    }

    public static class OdpsFieldWriter {
        public void write(FieldWriter fieldWriter, Object odpsObject) {

        }

        public void write(FieldWriter fieldWriter, Object odpsObject, FieldVector fieldVector) {
            write(fieldWriter, odpsObject);
        }
    }

    static class ByteWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            Byte data = (Byte) odpsObject;
            fieldWriter.writeTinyInt(data);
        }
    }

    static class ShortWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            short data = (short) odpsObject;
            fieldWriter.writeSmallInt(data);
        }
    }

    static class IntegerWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            int data = (int) odpsObject;
            fieldWriter.writeInt(data);
        }
    }

    static class LongWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            long data = (long) odpsObject;
            fieldWriter.writeBigInt(data);
        }
    }

    static class FloatWriter extends OdpsFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator(Integer.MAX_VALUE);

        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            Float data = (Float) odpsObject;
            byte[] value = String.format("%.6f", data).getBytes();
            ArrowBuf buf = bufferAllocator.buffer(value.length);
            buf.setBytes(0, value);

            fieldWriter.writeVarChar(0, value.length, buf);
            buf.close();
        }
    }

    static class DoubleWriter extends OdpsFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator(Integer.MAX_VALUE);

        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            double data = (double) odpsObject;
            byte[] value = String.format("%.15f", data).getBytes();
            ArrowBuf buf = bufferAllocator.buffer(value.length);
            buf.setBytes(0, value);

            fieldWriter.writeVarChar(0, value.length, buf);
            buf.close();
        }
    }

    static class BigDecimalWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            BigDecimal data = (BigDecimal) odpsObject;
            data = data.setScale(18, RoundingMode.HALF_UP);
            fieldWriter.writeDecimal(data);
        }
    }

    static class BooleanWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            boolean data = (boolean) odpsObject;
            fieldWriter.writeBit(data ? 1 : 0);
        }
    }

    static class StringWriter extends OdpsFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator(Integer.MAX_VALUE);

        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            byte[] data = ((String) odpsObject).getBytes();
            ArrowBuf buf = bufferAllocator.buffer(data.length);
            buf.setBytes(0, data);

            fieldWriter.writeVarChar(0, data.length, buf);
            buf.close();
        }
    }

    static class VarcharWriter extends OdpsFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator(Integer.MAX_VALUE);

        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            Varchar value = (Varchar) odpsObject;
            byte[] data = value.getValue().getBytes();
            ArrowBuf buf = bufferAllocator.buffer(data.length);
            buf.setBytes(0, data);

            fieldWriter.writeVarChar(0, data.length, buf);
            buf.close();
        }
    }

    static class BinaryWriter extends OdpsFieldWriter {
        BufferAllocator bufferAllocator = new RootAllocator(Integer.MAX_VALUE);

        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            byte[] data = ((Binary) odpsObject).data();
            ArrowBuf buf = bufferAllocator.buffer(data.length);
            buf.setBytes(0, data);

            fieldWriter.writeVarBinary(0, data.length, buf);
            buf.close();
        }
    }

    static class DateWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            java.util.Date data = (java.util.Date) odpsObject;
            fieldWriter.writeDateMilli(data.getTime());
        }
    }

    static class TimestampWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            java.sql.Timestamp data = (java.sql.Timestamp) odpsObject;
            fieldWriter.writeTimeStampNano(data.getTime());
        }
    }

    static class ListWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            List<?> data = (List<?>) odpsObject;

            fieldWriter.startList();

            for (Object el : data) {
                OdpsArrowWriter.write(fieldWriter, el);
            }

            fieldWriter.endList();
        }
    }

    static class MapWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter _ignore, Object odpsObject, FieldVector fieldVector) {
            UnionMapWriter writer = new UnionMapWriter((MapVector) fieldVector);
            writer.startMap();
            Map<?, ?> map = (Map<?, ?>) odpsObject;

            for (Object key : map.keySet()) {
                writer.startEntry();

                Object value = map.get(key);
                OdpsArrowWriter.write(writer.key(), key);
                OdpsArrowWriter.write(writer.value(), value);

                writer.endEntry();
            }

            writer.endMap();
            writer.clear();
        }
    }

    static class StructWriter extends OdpsFieldWriter {
        @Override
        public void write(FieldWriter fieldWriter, Object odpsObject) {
            Struct data = (Struct) odpsObject;

            fieldWriter.start();

            for (int i = 0, n = data.getFieldCount(); i < n; i++) {
                String fieldName = data.getFieldName(i);
                Object value = data.getFieldValue(i);

                FieldWriter structFieldWriter = StructFieldWriterGetter.getWriter(
                        fieldWriter,
                        fieldName,
                        data.getFieldTypeInfo(i).getOdpsType()
                );

                OdpsArrowWriter.write(structFieldWriter, value);
            }

            fieldWriter.end();
        }
    }
}
