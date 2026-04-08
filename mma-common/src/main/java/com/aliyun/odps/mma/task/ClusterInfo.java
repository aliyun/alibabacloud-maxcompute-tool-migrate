package com.aliyun.odps.mma.task;

import java.util.List;

import com.aliyun.odps.Table;
import com.aliyun.odps.mma.meta.schema.DstOdpsTableSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClusterInfo {

    public enum ClusterType {
        range,
        hash
    }

    public static final long DEFAULT_BUCKET_SIZE = 512;

    private List<String> columnName;
    private long buckets;
    private ClusterType clusterType;

    private List<Table.SortColumn> sortedBy;

    public ClusterInfo(List<String> columnName, int buckets) {
        this.columnName = columnName;
        this.buckets = buckets;
    }

    public boolean isRangeCluster() {
        return clusterType == ClusterType.range;
    }

    public boolean isHashCluster() {
        return clusterType == ClusterType.hash;
    }
}
