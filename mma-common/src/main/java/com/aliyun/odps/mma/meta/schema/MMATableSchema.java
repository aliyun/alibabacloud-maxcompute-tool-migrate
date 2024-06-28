package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.mma.util.ListUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
public class MMATableSchema {
    private String name;
    private String comment;
    private List<MMAColumnSchema> columns;
    private List<MMAColumnSchema> partitions;
    private List<MMATableConstraint> tableConstraints;
    private Boolean enableTransaction;

    public MMATableSchema(String name) {
        this.name = name;
    }

    public MMATableSchema(String name, List<MMAColumnSchema> cols, List<MMAColumnSchema> partitions) {
        this.name = name;
        this.columns = cols;
        this.partitions = partitions;
    }

    public MMATableSchema(String name, String comment, List<MMAColumnSchema> cols, List<MMAColumnSchema> partitions, List<MMATableConstraint> tableConstraints) {
        this.name = name;
        this.comment = comment;
        this.columns = cols;
        this.partitions = partitions;
        this.tableConstraints = tableConstraints;
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

    @JsonIgnore
    public List<String> getPrimaryKeys() {
        List<String> primaryKeys = new ArrayList<>();

        if (ListUtils.size(tableConstraints) == 0) {
            return primaryKeys;
        }

        if (Objects.nonNull(tableConstraints.get(0).getPrimaryKeys())) {
            primaryKeys = tableConstraints.get(0).getPrimaryKeys();
        }

        return primaryKeys;
    }

    @JsonIgnore
    public MMAForeignKeyConstraint getForeignKeyConstrat() {
        if (ListUtils.size(tableConstraints) == 0) {
            return null;
        }

        if (Objects.nonNull(tableConstraints.get(0).getForeignKeyConstraint())) {
            return tableConstraints.get(0).getForeignKeyConstraint();
        }

        return null;
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
                ListUtils.equals(this.partitions, other.partitions) &&
                ListUtils.equals(this.tableConstraints, other.tableConstraints);
    }
}
