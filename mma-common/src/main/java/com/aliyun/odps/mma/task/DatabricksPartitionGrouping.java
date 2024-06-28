package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.constant.SourceType;

public class DatabricksPartitionGrouping extends CommonPartitionGrouping {
    public DatabricksPartitionGrouping(int maxPartitionNum, int maxPartitionSize) {
        super(maxPartitionNum, maxPartitionSize);
    }

    public SourceType sourceType() {
        return SourceType.DATABRICKS;
    }
}
