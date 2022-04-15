package com.aliyun.odps.mma.server.action.info;

import java.util.List;

import com.aliyun.odps.utils.StringUtils;

public class CopyTaskActionInfo extends AbstractActionInfo {
  private String instanceId;
  private String logView;
  private Float progress;
  private List<List<Object>> result;

  public synchronized String getInstanceId() {
    return instanceId;
  }

  public synchronized String getLogView() {
    return logView;
  }

  public synchronized Float getProgress() {
    return progress;
  }

  public synchronized List<List<Object>> getResult() {
    return result;
  }

  public synchronized void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public synchronized void setLogView(String logView) {
    this.logView = logView;
  }

  public synchronized void setProgress(Float progress) {
    this.progress = progress;
  }

  public void setResult(List<List<Object>> result) {
    this.result = result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    sb.append(this.getClass().getSimpleName());
    String instanceId = getInstanceId();
    if (!StringUtils.isNullOrEmpty(instanceId)) {
      sb.append(" ").append(instanceId);
    }
    sb.append("]");

    return sb.toString();
  }
}
