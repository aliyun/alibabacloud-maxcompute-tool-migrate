package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.model.PartitionModel;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;

public class MergedPartitionGrouping implements PartitionGrouping {
    int maxPtLevel;
    long maxPtNum;
    PartitionGrouping defaultGrouping;

    public MergedPartitionGrouping(int maxPtLevel, long maxPtNum, PartitionGrouping defaultGrouping) {
        this.maxPtLevel = maxPtLevel;
        this.defaultGrouping = defaultGrouping;
        this.maxPtNum = maxPtNum;

        if (maxPtLevel < 0) {
            throw new RuntimeException("maxPtLevel must >= 0");
        }
    }

    @Override
    public List<List<PartitionModel>> group(List<PartitionModel> partitions) {
        PartitionModel pt0 = partitions.get(0);
        if (pt0.getValue().split("/").length <= this.maxPtLevel) {
            return defaultGrouping.group(partitions);
        }

        List<List<PartitionModel>> groups = new LinkedList<>();
        Map<String, List<PartitionModel>> ptToGroup = new HashMap<>();

        if (this.maxPtLevel == 0) {
            groups.add(partitions);
            return groups;
        }

        for (PartitionModel pm: partitions) {
            String[] values = pm.getValue().split("/");

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < this.maxPtLevel; i ++) {
                sb.append(values[i]).append("/");
            }

            String mergedValue = sb.toString();

            List<PartitionModel> group = ptToGroup.get(mergedValue);
            if (Objects.isNull(group)) {
                group = new ArrayList<>();
                group.add(pm);
                ptToGroup.put(mergedValue, group);
            } else {
                group.add(pm);
            }
        }

        long num = 0;
        List<PartitionModel> tempGroup = new ArrayList<>();
        for (String mergeValue: ptToGroup.keySet()) {
            List<PartitionModel> group = ptToGroup.get(mergeValue);
            tempGroup.addAll(ptToGroup.get(mergeValue));
            num += group.size();

            if (num >= maxPtNum) {
                num = 0;
                groups.add(tempGroup);
                tempGroup = new ArrayList<>();
            }
        }

        if (! tempGroup.isEmpty()) {
            groups.add(tempGroup);
        }

        return groups;
    }
}
