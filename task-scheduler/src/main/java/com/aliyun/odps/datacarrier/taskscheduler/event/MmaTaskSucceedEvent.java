package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.Objects;

public class MmaTaskSucceedEvent extends BaseMmaEvent {

  private String taskId;
  private Integer numOfPartitions;

  public MmaTaskSucceedEvent(String taskId) {
    this(taskId, null);
  }

  public MmaTaskSucceedEvent(String taskId, Integer numOfPartitions) {
    this.taskId = Objects.requireNonNull(taskId);
    this.numOfPartitions = numOfPartitions;
  }

  @Override
  public MmaEventType getType() {
    return MmaEventType.JOB_SUCCEEDED;
  }

  @Override
  public String toString() {
    if (numOfPartitions == null) {
      return String.format("Task succeeded: %s", taskId);
    } else {
      return String.format("Task succeeded: %s(includes %d partitions)", taskId, numOfPartitions);
    }
  }
}
