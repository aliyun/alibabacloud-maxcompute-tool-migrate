package com.aliyun.odps.mma.query;

import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Data
public class DbFilter extends QueryFilter {
    private int id;
    private int sourceId;
    private String name;
    private String status;
    private TableFilter.TableSorter sorter;

    public List<SortPair> getOrders() {
        if (Objects.nonNull(sorter)) {
            return sorter.orders();
        }

        return Collections.emptyList();
    }

    @Data
    public static class DbSorter extends Sorter {
        @FiledName("num_rows")
        private String numRows;
        @FiledName("size")
        private String size;
        @FiledName("update_time")
        private String updateTime;
    }
}
