package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.util.TableHasher;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.Objects;

@Data
public class PartitionModel extends ModelBase {
    private Integer id;
    private Integer DbId;
    private Integer tableId;
    private String dbName;
    private String tableName;
    private String value;
    private String inputFormat;
    private String outputFormat;
    private String serde;

    public TableHasher tableTableHasher() {
        return new TableHasher(sourceId, dbName, tableName, DbId, tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, value);
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof PartitionModel)) {
            return false;
        }

        PartitionModel o = (PartitionModel) other;

        return Objects.equals(this.dbName, o.dbName)
                && Objects.equals(this.tableName, o.tableName)
                && Objects.equals(this.value, o.value);
    }

    @JsonIgnore
    public String key() {
        return String.format("%s.%s.%s", dbName, tableName, value);
    }
}
