package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.constant.TaskType;

import java.util.concurrent.TimeUnit;

public class MockTaskExecutor extends TaskExecutor {
    public MockTaskExecutor() {
        super();
    }

    @Override
    public TaskType taskType() {
        return TaskType.MOCK;
    }

    @Override
    protected void _setUpSchema() throws Exception {
        TimeUnit.SECONDS.sleep(20);
        task.log("set up schema", "ok");
    }

    @Override
    protected void _dataTrans() throws Exception {
        TimeUnit.SECONDS.sleep(20);
        task.log("data trans", "ok");
    }

    @Override
    protected void _verifyData() throws Exception {
        TimeUnit.SECONDS.sleep(20);
        task.log("verify data", "ok");
    }
}
