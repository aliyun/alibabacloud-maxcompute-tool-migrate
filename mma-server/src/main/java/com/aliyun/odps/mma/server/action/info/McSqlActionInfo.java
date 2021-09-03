/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.server.action.info;

import java.util.List;

import com.aliyun.odps.utils.StringUtils;

public class McSqlActionInfo extends AbstractActionInfo {

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
