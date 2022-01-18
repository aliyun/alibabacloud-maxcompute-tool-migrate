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
 *
 */

package com.aliyun.odps.mma.meta.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Validate;

public class PartitionMetaModel {

  public PartitionMetaModel(
      List<String> partitionValues,
      String location,
      Long createTime,
      Long lastModificationTime,
      Long size) {
    this.partitionValues = new ArrayList<>(Validate.notNull(partitionValues));
    this.partitionValues.forEach(Validate::notNull);
    this.location = location;
    this.createTime = createTime;
    this.lastModificationTime = lastModificationTime;
    this.size = size;
  }

  private List<String> partitionValues;
  private String location;
  private Long createTime;
  private Long lastModificationTime;
  private Long size;

  public List<String> getPartitionValues() {
    return new ArrayList<>(partitionValues);
  }

  public String getLocation() {
    return location;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public Long getLastModificationTime() {
    return lastModificationTime;
  }

  public Long getSize() {
    return size;
  }
}
