package com.aliyun.odps.datacarrier.taskscheduler.job;

import java.util.List;

import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.datacarrier.taskscheduler.task.Task;

public interface Job {

  List<Task> getExecutableTasks();

  JobStatus getStatus();

  String getId();

  void stop();
}
