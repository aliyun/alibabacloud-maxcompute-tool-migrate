package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.List;
import java.util.Objects;

public class MmaTaskFailedEvent extends BaseMmaEvent {

  private String id;
  private List<String> actionIds;

  public MmaTaskFailedEvent(String id, List<String> actionIds) {
    this.id = Objects.requireNonNull(id);
    this.actionIds = actionIds;
  }

  @Override
  public MmaEventType getType() {
    return MmaEventType.TASK_FAILED;
  }

  @Override
  public String toString() {
    if (actionIds != null) {
      return String.format(
          "Task failed: %s (caused by %s)", id, String.join(", ", actionIds));
    } else {
      return String.format("Task failed: %s", id);
    }
  }
}
