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

package com.aliyun.odps.mma.server.job;

import java.util.Collections;
import java.util.List;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;

public class McToOssResourceJob extends AbstractJob {

  public McToOssResourceJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  Task task;

  @Override
  public List<Task> getExecutableTasks() {
    if (task == null && JobStatus.PENDING.equals(getStatus())) {
      task = generateTask();
    }
    if (task == null) {
      // Exception happened when generating the DAG.
      return Collections.emptyList();
    }

    if (TaskProgress.PENDING.equals(task.getProgress())) {
      return Collections.singletonList(task);
    } else {
      return Collections.emptyList();
    }
  }

  private Task generateTask() {
    // TODO:
    return null;
  }

  @Override
  boolean updateObjectMetadata() {
    // TODO:
    return false;
  }

  @Override
  public synchronized void setStatus(Task task) {

  }
}
