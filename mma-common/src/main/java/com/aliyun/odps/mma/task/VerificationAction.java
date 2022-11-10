package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.orm.TaskProxy;

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

    }
}
