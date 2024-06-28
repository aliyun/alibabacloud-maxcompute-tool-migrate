package com.aliyun.odps.mma.util;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
public class TableHasher {
    private String dbName;
    private String schemName;
    private String tableName;

    public TableHasher(String dbName, String schemName, String tableName) {
        this.dbName = dbName;
        this.schemName = schemName;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", dbName, schemName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, schemName, tableName);
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof TableHasher)) {
            return false;
        }

        TableHasher ob = (TableHasher) other;

        return Objects.equals(this.dbName, ob.dbName)
                && Objects.equals(this.schemName, ob.schemName)
                && Objects.equals(this.tableName, ob.tableName);
     }
}
