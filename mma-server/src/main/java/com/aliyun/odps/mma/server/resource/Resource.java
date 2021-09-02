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

package com.aliyun.odps.mma.server.resource;

public enum Resource {
  /**
   * Represents a worker of a data transmission job.
   *
   * In the case of Hive to MC, mapreduce.job.running.map.limit will be decided by the allocated
   * number of this resource.
   */
  DATA_WORKER,

  /**
   * Represents an operation on the metadata of a MaxCompute object (table, resource, e.g.).
   */
  METADATA_WORKER
}
