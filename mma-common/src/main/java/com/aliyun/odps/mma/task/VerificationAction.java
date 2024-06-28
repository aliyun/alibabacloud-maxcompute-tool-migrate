package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.orm.TaskProxy;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class VerificationAction {
    public static void countResultCompare(String srcName, long srcCnt, String destName, long destCnt, TaskProxy task)
            throws MMATaskInterruptException {
        String action = String.format("compare %s and %s count", srcName, destName);
        String errMsg = String.format("%s Count: %d, %s Count: %d", srcName, srcCnt, destName, destCnt);
        if (srcCnt == -1 || destCnt == -1) {
            task.error(action, errMsg);
            throw new MMATaskInterruptException();
        }

        if (srcCnt != destCnt) {
            task.error(action, errMsg);
            throw new MMATaskInterruptException();
        }

        task.log("select count", String.format("src: %d, dsc: %d", srcCnt, destCnt));
    }

    public static void countByPtResultCompare(String srcName, Map<String, Long> srcCount, String destName, Map<String, Long> dstCount, TaskProxy task)
            throws MMATaskInterruptException {
        String action = String.format("compare %s and %s count", srcName, destName);

        Set<String> srcKeys = srcCount.keySet();
        Set<String> dstKeys = dstCount.keySet();
        Set<String> keys = srcKeys;
        if (srcKeys.size() < dstKeys.size()) {
            keys = dstKeys;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("src table vs dst table:\n");
        boolean equals = true;
        for (String key: keys) {
            Long c1 = srcCount.getOrDefault(key, -1L);
            Long c2 = dstCount.getOrDefault(key, -1L);
            sb.append(String.format("%s: %d, %d", key, c1, c2));

            if (!Objects.equals(c1, c2)) {
                sb.append(" not equal!");
                equals = false;
            }

            sb.append("\n");
        }

        if (equals) {
            task.log(action, sb.toString());
        } else {
            task.error(action, sb.toString());
            throw new MMATaskInterruptException();
        }
    }
}
