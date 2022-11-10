package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.constant.TaskType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class TaskUtils {
    ApplicationContext appCtx;
    public Map<TaskType, Class<?>> taskExecutorMap = new HashMap<>();

    @Autowired
    public TaskUtils(ApplicationContext appCtx, List<TaskExecutorInter> interList) {
        this.appCtx = appCtx;

        for(TaskExecutorInter inter: interList) {
            taskExecutorMap.put(inter.taskType(), inter.getClass());
        }
    }

    public TaskExecutor getTaskExecutor(TaskType taskType) {
        Class<?> c = taskExecutorMap.get(taskType);
        return (TaskExecutor) this.appCtx.getBean(c);
    }
}
