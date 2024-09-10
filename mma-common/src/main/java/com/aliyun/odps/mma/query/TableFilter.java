package com.aliyun.odps.mma.query;

import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Data
public class TableFilter extends QueryFilter {
    private int sourceId;
    private int dbId;
    private String dbName;
    private String schemaName;
    private int updated = -1;
    private String name;
    private String type;
    private String hasPartitions;
    private List<String> status;
    private TableSorter sorter;
    private List<String> lastDdlTime;

    public List<SortPair> getOrders() {
        if (Objects.nonNull(sorter)) {
            return sorter.orders();
        }

        return Collections.emptyList();
    }

    @Data
    public static class TableSorter extends Sorter {
        @FiledName("num_rows")
        private String numRows;
        @FiledName("size")
        private String size;
        @FiledName("last_ddl_time")
        private String lastDdlTime;
    }

    public int getHasPartitions() {
        if (Objects.isNull(hasPartitions)) {
            return -1;
        }

        if ("yes".equals(hasPartitions)) {
            return 1;
        }

        return  0;
    }

    public String getLastDdlTimeStart() {
        return lastDdlTime.get(0);
    }

    public String getLastDdlTimeEnd() {
        return lastDdlTime.get(1);
    }
}
