package com.aliyun.odps.mma.util.id;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TaskIdGen extends ModelIdGen {
    public TaskIdGen(@Value("${MMA_TASK_ID_CACHE_SIZE:1000}") int cacheSize) {
        super("task", cacheSize);
    }
}
