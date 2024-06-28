package com.aliyun.odps.mma.util.id;

import org.springframework.stereotype.Component;

@Component
public class TaskIdGen extends ModelIdGen {
    public TaskIdGen() {
        super("task", 20);
    }
}
