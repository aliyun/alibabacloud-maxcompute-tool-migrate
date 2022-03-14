package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OdpsToArrowTypeConverter {

    public static Schema tableSchemaToArrowSchema(TableSchema tableSchema, List<Column> columns) {
        if (columns == null) {
            return tableSchemaToArrowSchema(tableSchema);
        } else {
            List<Field> fields = new ArrayList<>();
            for (Column column : columns) {
                Column fieldColumn = tableSchema.getColumn(column.getName());
                fields.add(columnToArrowField(fieldColumn));
            }
            return new Schema(fields);
        }
    }

    public static Schema tableSchemaToArrowSchema(TableSchema tableSchema) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < tableSchema.getColumns().size(); i++) {
            fields.add(columnToArrowField(tableSchema.getColumn(i)));
        }
        return new Schema(fields);
    }

    public static Field columnToArrowField(Column fieldColumn) {
        String fieldName = fieldColumn.getName();
        TypeInfo typeInfo = fieldColumn.getTypeInfo();
        // TODO 是否将nullable永远设置为true?
        return convertTypeInfoToArrowField(fieldName, typeInfo, fieldColumn.isNullable());
    }

    private static Field convertTypeInfoToArrowField(String fieldName, TypeInfo typeInfo, boolean nullable) {
        ArrowType arrowType = getArrowType(typeInfo);
        return new Field(fieldName, new FieldType(nullable, arrowType, null, null), generateSubFields(typeInfo));
    }

    private static List<Field> generateSubFields(TypeInfo typeInfo) {
        if (typeInfo instanceof ArrayTypeInfo) {
            ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo)typeInfo;
            TypeInfo subti = arrayTypeInfo.getElementTypeInfo();
            return Arrays.asList(convertTypeInfoToArrowField("element", subti, true));
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;
            TypeInfo keyti = mapTypeInfo.getKeyTypeInfo(), valti = mapTypeInfo.getValueTypeInfo();
            return Arrays.asList(
                    new Field("element", new FieldType(false, new ArrowType.Struct(), null, null),
                            Arrays.asList(
                                    convertTypeInfoToArrowField("key", keyti, false),
                                    convertTypeInfoToArrowField("value", valti, true)
                            )
                    )
            );
        } else if (typeInfo instanceof StructTypeInfo) {
            StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo;
            ArrayList<Field> sfields = new ArrayList<>();
            List<TypeInfo> subTypeInfos = structTypeInfo.getFieldTypeInfos();
            List<String> subNames = structTypeInfo.getFieldNames();
            for(int i = 0; i < structTypeInfo.getFieldCount(); i++) {
                sfields.add(convertTypeInfoToArrowField(subNames.get(i), subTypeInfos.get(i), true));
            }
            return sfields;
        } else {
            return null;
        }
    }


    private static ArrowType getArrowType(TypeInfo typeInfo) {
        ArrowType arrowType = null;
        switch (typeInfo.getOdpsType()) {
            case CHAR:
            case VARCHAR:
            case STRING:
            case FLOAT:
            case DOUBLE:
                arrowType = new ArrowType.Utf8();
                break;
            case BINARY:
                arrowType = new ArrowType.Binary();
                break;
            case TINYINT:
                arrowType = new ArrowType.Int(8, true);
                break;
            case SMALLINT:
                arrowType = new ArrowType.Int(16, true);
                break;
            case INT:
                arrowType = new ArrowType.Int(32, true);
                break;
            case BIGINT:
                arrowType = new ArrowType.Int(64, true);
                break;
            case BOOLEAN:
                arrowType = new ArrowType.Bool();
                break;
            case DECIMAL:
                arrowType = new ArrowType.Decimal(((DecimalTypeInfo) typeInfo).getPrecision(), ((DecimalTypeInfo) typeInfo).getScale());
                break;
            case DATE:
                arrowType = new ArrowType.Date(DateUnit.DAY);
                break;
            case DATETIME:
                arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
                break;
            case TIMESTAMP:
                arrowType = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
                break;
            case ARRAY:
                arrowType = new ArrowType.List();
                break;
            case INTERVAL_DAY_TIME:
                arrowType = new ArrowType.Interval(IntervalUnit.DAY_TIME);
                break;
            case INTERVAL_YEAR_MONTH:
                arrowType = new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
                break;
            case STRUCT:
                arrowType = new ArrowType.Struct();
                break;
            case MAP:
                arrowType = new ArrowType.Map(true);
                break;
            case VOID:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + typeInfo.getOdpsType());
        }
        return arrowType;
    }
}