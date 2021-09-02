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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class VerificationActionInfo extends AbstractActionInfo {
  private List<String> succeededPartitions = new LinkedList<>();
  private List<String> failedPartitions = new LinkedList<>();
  private Map<String, Long> partitionValuesToSourceNumRecord = new HashMap<>();
  private Map<String, Long> partitionValuesToDestNumRecord = new HashMap<>();
  private Long sourceNumRecord;
  private Long destNumRecord;
  private Boolean passed;
  private Boolean isPartitioned;

  public Boolean isPartitioned() {
    return isPartitioned;
  }

  public Boolean passed() {
    return passed;
  }

  public List<String> getSucceededPartitions() {
    return succeededPartitions;
  }

  public List<String> getFailedPartitions() {
    return failedPartitions;
  }

  public void setPassed(Boolean passed) {
    this.passed = passed;
  }

  public void setIsPartitioned(Boolean isPartitioned) {
    this.isPartitioned = isPartitioned;
  }

  public void setSucceededPartitions(List<String> succeededPartitions) {
    this.succeededPartitions = succeededPartitions;
  }

  public void setFailedPartitions(List<String> failedPartitions) {
    this.failedPartitions = failedPartitions;
  }

  public void setSourceNumRecord(Long sourceNumRecord) {
    this.sourceNumRecord = sourceNumRecord;
  }

  public Long getSourceNumRecord() {
    return sourceNumRecord;
  }

  public void setDestNumRecord(Long destNumRecord) {
    this.destNumRecord = destNumRecord;
  }

  public Long getDestNumRecord() {
    return destNumRecord;
  }

  public void setPartitionValuesToSourceNumRecord(
      String partitionValuesStr, Long numRecord) {
    this.partitionValuesToSourceNumRecord.put(partitionValuesStr, numRecord);
  }

  public Map<String, Long> getPartitionValuesToSourceNumRecord() {
    return partitionValuesToSourceNumRecord;
  }

  public void setPartitionValuesToDestNumRecord(String partitionValuesStr, Long numRecord) {
    this.partitionValuesToDestNumRecord.put(partitionValuesStr, numRecord);
  }

  public Map<String, Long> getPartitionValuesToDestNumRecord() {
    return partitionValuesToDestNumRecord;
  }

  @Override
  public String toString() {
    //TODO: to string
    return null;
  }
}
