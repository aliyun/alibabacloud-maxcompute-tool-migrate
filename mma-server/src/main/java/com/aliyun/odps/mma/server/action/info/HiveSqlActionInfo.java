/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.mma.server.action.info;

import java.util.List;
import java.util.Objects;

import com.aliyun.odps.utils.StringUtils;

public class HiveSqlActionInfo extends AbstractActionInfo {

  private String jobId;
  private String trackingUrl;
  private Float progress;
  private List<Integer> resultSchema;
  private List<List<Object>> result;

  public synchronized String getJobId() {
    return jobId;
  }

  public synchronized String getTrackingUrl() {
    return trackingUrl;
  }

  public synchronized Float getProgress() {
    return progress;
  }

  public synchronized List<Integer> getResultSchema() {
    return resultSchema;
  }

  public synchronized List<List<Object>> getResult() {
    return result;
  }

  public synchronized void setJobId(String jobId) {
    this.jobId = Objects.requireNonNull(jobId);
  }

  public synchronized void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = Objects.requireNonNull(trackingUrl);
  }

  public synchronized void setProgress(Float progress) {
    this.progress = progress;
  }

  public synchronized void setResultSchema(List<Integer> resultSchema) {
    this.resultSchema = resultSchema;
  }

  public synchronized void setResult(List<List<Object>> result) {
    this.result = result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    sb.append(this.getClass().getSimpleName());
    String jobId = getJobId();
    if (!StringUtils.isNullOrEmpty(jobId)) {
      sb.append(" ").append(jobId);
    }
    sb.append("]");

    return sb.toString();
  }
}
