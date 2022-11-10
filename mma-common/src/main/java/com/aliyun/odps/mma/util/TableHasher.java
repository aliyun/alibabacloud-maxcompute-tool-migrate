package com.aliyun.odps.mma.util;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
public class TableHasher {
    private Integer sourceId;
    private String dbName;
    private String tableName;
    private Integer dbId;
    private Integer tableId;

    public TableHasher(Integer sourceId, String dbName, String tableName, Integer dbId, Integer tableId) {
        this.sourceId = sourceId;
        this.dbName = dbName;
        this.tableName = tableName;
        this.dbId = dbId;
        this.tableId = tableId;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", dbName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceId, dbName, tableName);
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof TableHasher)) {
            return false;
        }

        TableHasher ob = (TableHasher) other;

        return Objects.equals(this.sourceId, ob.sourceId) && Objects.equals(this.dbName, ob.dbName) && Objects.equals(this.tableName, ob.tableName);
     }
}
