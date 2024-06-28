package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.model.PartitionModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class CommonPartitionGrouping implements PartitionGrouping {
    private final long maxPartitionNum;
    private final long maxPartitionSize;

    public CommonPartitionGrouping(int maxPartitionNum, int maxPartitionSize) {
        this.maxPartitionNum = maxPartitionNum;
        this.maxPartitionSize = (long)maxPartitionSize * 1024 * 1024 * 1024;
    }

    @Override
    public List<List<PartitionModel>> group(List<PartitionModel> partitions) {
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        // 按照大小倒序排列
        //partitions.sort((p1, p2) -> (int)(p2.getSizeOpt().orElse(0L) - p1.getSizeOpt().orElse(0L)));

        long size = 0L;
        int num = 0;

        List<List<PartitionModel>> groups = new LinkedList<>();
        List<PartitionModel> group = new ArrayList<>();

        if (maxPartitionNum <= 0 && maxPartitionSize <=0) {
            groups.add(partitions);
            return groups;
        }

        for (PartitionModel pm: partitions) {
            size += pm.getSizeOpt().orElse(0L);
            num += 1;

            group.add(pm);

            if ((maxPartitionSize > 0 && size >= maxPartitionSize) || num >= maxPartitionNum) {
                groups.add(group);
                group = new ArrayList<>();
                size = 0;
                num = 0;
            }
        }

        if (!group.isEmpty()) {
            groups.add(group);
        }

         return groups;
    }

    public SourceType sourceType() {
        return SourceType.HIVE;
    }
}
