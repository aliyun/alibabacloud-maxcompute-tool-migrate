package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.model.PartitionModel;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OdpsPartitionGrouping implements PartitionGrouping {
    @Override
    public List<List<PartitionModel>> group(List<PartitionModel> partitions) {
        return partitions.stream().map(p -> {
            ArrayList<PartitionModel> a = new ArrayList<>(1);
            a.add(p);

            return a;
        }).collect(Collectors.toList());
    }
}
