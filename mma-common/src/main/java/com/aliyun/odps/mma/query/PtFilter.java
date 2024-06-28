package com.aliyun.odps.mma.query;

import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Data
public class PtFilter extends QueryFilter {
    private int sourceId;
    private int dbId;
    private String dbName;
    private String schemaName;
    private int updated = -1;
    private String tableName;
    private String value;
    private List<String> status;
    private List<String> lastDdlTime;
    private PtSorter sorter;

    public String getLastDdlTimeStart() {
        return lastDdlTime.get(0);
    }

    public String getLastDdlTimeEnd() {
        return lastDdlTime.get(1);
    }

    public List<SortPair> getOrders() {
        if (Objects.nonNull(sorter)) {
            return sorter.orders();
        }

        return Collections.emptyList();
    }

    @Data
    public static class PtSorter extends Sorter {
        @FiledName("num_rows")
        private String numRows;
        @FiledName("size")
        private String size;
        @FiledName("last_ddl_time")
        private String lastDdlTime;
    }
}
