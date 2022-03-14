package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OdpsColumn {
    private String name;
    private TypeInfo typeInfo;
    private boolean nullable;

    public OdpsColumn(Column column) {
        name = column.getName();
        typeInfo = column.getTypeInfo();
        nullable = column.isNullable();
    }

    public OdpsColumn(String name, String typeName, String isNullable) {
        this.name = name;
        this.typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeName);
        this.nullable = "true".equals(isNullable);
    }

    public static String toJsonArray(List<OdpsColumn> columnList) {
        Gson gson = new GsonBuilder()
                .disableHtmlEscaping()
                .registerTypeAdapter(OdpsColumn.class, new OdpsColumnGsonSerializer())
                .create();

        return gson.toJson(columnList,  new TypeToken<List<OdpsColumn>>() {}.getType());
    }

    public static List<OdpsColumn> fromJsonArray(String json) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(OdpsColumn.class, new OdpsColumnGsonDeserializer())
                .create();

        return gson.fromJson(json,  new TypeToken<List<OdpsColumn>>() {}.getType());
    }

    public static List<Column> toColumns(List<OdpsColumn> columns) {
        List<Column> ret = new ArrayList<>(columns.size());
        for (OdpsColumn oc: columns) {
            ret.add(oc.toColumn());
        }

        return ret;
    }

    public Column toColumn() {
        Column c = new Column(name, typeInfo);
        c.setNullable(nullable);

        return c;
    }

    public String getName() {
        return name;
    }

    public TypeInfo getTypeInfo() {
        return typeInfo;
    }

    public boolean getNullable() {
        return nullable;
    }

    public static void main(String[] args) {
        Column c1 = new Column("c1", TypeInfoParser.getTypeInfoFromTypeString("string"));
        Column c2 = new Column("c2", TypeInfoParser.getTypeInfoFromTypeString("struct<x:int,y:varchar(256),z:struct<a:tinyint,b:date>>"));

        List<OdpsColumn> ocList_ = Arrays.asList(new OdpsColumn(c1), new OdpsColumn(c2));
        List<OdpsColumn> ocList = OdpsColumn.fromJsonArray(OdpsColumn.toJsonArray(ocList_));

        OdpsColumn oc1 = ocList.get(0);
        OdpsColumn oc2 = ocList.get(1);

        assert oc1.name.equals(c1.getName());
        assert oc1.typeInfo.getTypeName().equals(c1.getTypeInfo().getTypeName());

        assert oc2.name.equals(c2.getName());
        assert oc2.typeInfo.getTypeName().equals(c2.getTypeInfo().getTypeName());

        List<OdpsColumn> odpsColumns = new ArrayList<>();
        odpsColumns.add(new OdpsColumn(new Column("t_float", TypeInfoParser.getTypeInfoFromTypeString("float"))));
        odpsColumns.add(new OdpsColumn(new Column("t_double", TypeInfoParser.getTypeInfoFromTypeString("double"))));

        System.out.println(OdpsColumn.toJsonArray(odpsColumns));
    }
}

class OdpsColumnGsonSerializer implements JsonSerializer<OdpsColumn> {
    @Override
    public JsonElement serialize(OdpsColumn odpsColumn, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject jsonObject = new JsonObject();

        jsonObject.addProperty("name", odpsColumn.getName());
        jsonObject.addProperty("type", odpsColumn.getTypeInfo().getTypeName());
        jsonObject.addProperty("nullable",  odpsColumn.getNullable());

        return jsonObject;
    }
}

class OdpsColumnGsonDeserializer implements JsonDeserializer<OdpsColumn> {
    @Override
    public OdpsColumn deserialize(JsonElement json, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();

        String name = jsonObject.get("name").getAsString();
        String typeName = jsonObject.get("type").getAsString();
        String nullable = jsonObject.get("nullable").getAsString();

        return new OdpsColumn(name, typeName, nullable);
    }
}