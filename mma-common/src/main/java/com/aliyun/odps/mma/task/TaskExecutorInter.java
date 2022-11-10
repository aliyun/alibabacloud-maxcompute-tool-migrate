package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.constant.TaskType;

public interface TaskExecutorInter {
    TaskType taskType();
    void setUpSchema() throws Exception;
    void dataTrans() throws Exception;
    void verifyData() throws Exception;
}
