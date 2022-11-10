package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.model.TaskModel;
import org.springframework.context.ApplicationEvent;

public class TaskEvent extends ApplicationEvent {
    TaskModel taskModel;

    public TaskEvent(Object source, TaskModel taskModel) {
        super(source);
        this.taskModel = taskModel;
    }

    public TaskModel getTaskModel() {
        return this.taskModel;
    }
}
