package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.constant.SourceType;

public class HiveGluePartitionGrouping extends CommonPartitionGrouping {
    public HiveGluePartitionGrouping(int maxPartitionNum, int maxPartitionSize) {
        super(maxPartitionNum, maxPartitionSize);
    }

    public SourceType sourceType() {
        return SourceType.HIVE_GLUE;
    }
}
