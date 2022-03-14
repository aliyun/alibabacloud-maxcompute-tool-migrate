package com.aliyun.odps.mma.validation;

import com.aliyun.odps.OdpsType;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;

public class StructFieldWriterGetter {
    public static FieldWriter getWriter(BaseWriter.StructWriter structWriter, String fieldName, OdpsType odpsType) {
        switch (odpsType) {
            case CHAR:
            case VARCHAR:
            case STRING:
                return (FieldWriter) structWriter.varChar(fieldName);
            case BINARY:
                return (FieldWriter) structWriter.varBinary(fieldName);
            case TINYINT:
                return (FieldWriter) structWriter.tinyInt(fieldName);
            case SMALLINT:
                return (FieldWriter) structWriter.smallInt(fieldName);
            case INT:
                return (FieldWriter) structWriter.integer(fieldName);
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return (FieldWriter) structWriter.bigInt(fieldName);
            case BOOLEAN:
                return (FieldWriter) structWriter.bit(fieldName);
            case DECIMAL:
                return (FieldWriter) structWriter.decimal(fieldName);
            case DATE:
                return (FieldWriter) structWriter.dateDay(fieldName);
            case DATETIME:
                return (FieldWriter) structWriter.dateMilli(fieldName);
            case TIMESTAMP: {
                return (FieldWriter) structWriter.timeMilli(fieldName);
            }
            case ARRAY:
                return (FieldWriter) structWriter.list(fieldName);
            case MAP:
                throw new RuntimeException("arrow doesn't support nested map type");
            case STRUCT:
                return (FieldWriter) structWriter.struct(fieldName);
        }

        throw new RuntimeException(String.format("cannot handle %s type", odpsType.name()));
    }
}
