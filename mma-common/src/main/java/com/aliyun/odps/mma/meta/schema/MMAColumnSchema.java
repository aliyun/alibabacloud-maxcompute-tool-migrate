package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.Column;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;

import java.util.Objects;

@Getter
public class MMAColumnSchema {
    private final String name;
    private final String type;
    private final String comment;
    private final String defaultValue;
    private final Boolean nullable;

    public MMAColumnSchema(String name, String type, String comment) {
        this(name, type, comment, null, null);
    }

    public MMAColumnSchema(String name, String type, String comment, String defaultValue, Boolean nullable) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.defaultValue = defaultValue;
        this.nullable = nullable;
    }

    public String toJson() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(this);
    }

    public static MMAColumnSchema fromJson(String json) {
        Gson gson = new GsonBuilder().create();

        return gson.fromJson(json, MMAColumnSchema.class);
    }

    @Override
    public boolean equals(Object ot) {
        if (! (ot instanceof MMAColumnSchema)) {
            return  false;
        }

        MMAColumnSchema other = (MMAColumnSchema) ot;

        return Objects.equals(this.name, other.getName()) &&
                Objects.equals(this.type, other.getType()) &&
                Objects.equals(this.comment, other.getComment()) &&
                Objects.equals(this.defaultValue, other.getDefaultValue()) &&
                Objects.equals(this.nullable, other.getNullable());
    }

    public static MMAColumnSchema fromOdpsColumn(Column column) {
        return new MMAColumnSchema(
                column.getName(),
                column.getTypeInfo().getTypeName(),
                column.getComment()
        );
    }
}
