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

package com.aliyun.odps.mma.server.task;

import java.util.List;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public abstract class TableDataTransmissionTask extends DagTask {

  TableMetaModel source;
  TableMetaModel dest;
  Job job;
  List<Job> subJobs;

  public TableDataTransmissionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel source,
      TableMetaModel dest,
      Job job,
      List<Job> subJobs) {
    super(id, rootJobId, config);
    this.source = source;
    this.dest = dest;
    this.job = job;
    this.subJobs = subJobs;
  }

  public List<Job> getSubJobs() {
    return subJobs;
  }
}
