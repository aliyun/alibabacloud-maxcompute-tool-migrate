package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.model.PartitionModel;

import java.util.List;

public interface PartitionGrouping {
    List<List<PartitionModel>> group(List<PartitionModel> partitions);
}
