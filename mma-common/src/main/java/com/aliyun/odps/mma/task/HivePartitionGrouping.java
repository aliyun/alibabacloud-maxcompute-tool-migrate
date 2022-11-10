package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.constant.SourceType;

public class HivePartitionGrouping extends CommonPartitionGrouping {
    public HivePartitionGrouping(int maxPartitionNum, int maxPartitionSize) {
        super(maxPartitionNum, maxPartitionSize);
    }

    public SourceType sourceType() {
        return SourceType.HIVE;
    }
}
