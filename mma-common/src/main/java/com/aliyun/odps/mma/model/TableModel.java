package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.util.TableHasher;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.gson.Gson;
import lombok.Data;

import java.util.Map;
import java.util.Objects;

@Data
public class TableModel extends ModelBase {
    private Integer id;
    private Integer dbId;
    private String dbName;
    private String schemaName;
    private String name;
    private String type;
    private Integer lifecycle;
    private boolean hasPartitions;
    private MMATableSchema schema;
    private String owner;
    private String location;
    private String inputFormat;
    private String outputFormat;
    private String serde;

    // 非数据库字段
    private int partitions;
    private int partitionsDoing;
    private int partitionsDone;
    private int partitionsFailed;

    public String getNameWithSchema() {
        if (Objects.nonNull(schema)) {
            return schemaName + "." + name;
        }

        return name;
    }

    @JsonIgnore
    public TableHasher getTableHasher() {
        return new TableHasher(dbName, schemaName, name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, schemaName, name);
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof TableModel)) {
            return false;
        }

        TableModel o = (TableModel) other;

        return Objects.equals(this.dbName, o.dbName)
                && Objects.equals(this.schemaName, o.schemaName)
                && Objects.equals(this.name, ((TableModel) other).name);
    }

    @JsonIgnore
    public boolean hasDecimalColumn() {
        return this.schema.getColumns().stream().anyMatch(c -> c.getType().toLowerCase().startsWith("decimal"));
    }

    @JsonIgnore
    public boolean decimalOdps2() {
        return this.schema.getColumns().stream().anyMatch(c -> c.getType().toLowerCase().startsWith("decimal("));
    }

    @JsonIgnore
    public String getFullName() {
        if (Objects.nonNull(schemaName)) {
            return dbName + "." + schemaName + "." + name;
        }

        return dbName + "." + name;
    }

    public void setExtraMap(Map<String, String> extraMap) {
        Gson gson = new Gson();
        extra = gson.toJson(extraMap);
    }
}
