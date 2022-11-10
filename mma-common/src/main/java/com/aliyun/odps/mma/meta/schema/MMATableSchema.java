package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.mma.util.ListUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Objects;

@Getter
@Setter
public class MMATableSchema {
    private String name;
    private String comment;
    private List<MMAColumnSchema> columns;
    private List<MMAColumnSchema> partitions;

    public MMATableSchema(String name) {
        this.name = name;
    }

    public MMATableSchema(String name, List<MMAColumnSchema> cols, List<MMAColumnSchema> partitions) {
        this.name = name;
        this.columns = cols;
        this.partitions = partitions;
    }

    private List<String> partitionValues;

    public String toJson() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(this);
    }

    public static MMATableSchema fromJson(String json) {
        Gson gson = new GsonBuilder().create();

        return gson.fromJson(json, MMATableSchema.class);
    }

    @Override
    public boolean equals(Object ot) {
        if (! (ot instanceof MMATableSchema)) {
            return false;
        }

        MMATableSchema other = (MMATableSchema) ot;

        boolean eq = Objects.equals(this.name, other.name) &&
                Objects.equals(this.comment, other.comment);

        if (! eq) {
            return false;
        }

        return ListUtils.equals(this.columns, other.columns) &&
                ListUtils.equals(this.partitions, other.partitions);
    }
}
