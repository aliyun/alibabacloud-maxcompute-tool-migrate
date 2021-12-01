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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.task.Task;

/**
 * A handle of job.
 */
public interface Job {

  /**
   * return executable tasks for scheduler
   * don't return null
   * - exception => set fail status => return empty list
   * @return
   */
  List<Task> getExecutableTasks();

  List<Job> getSubJobs();

  JobStatus getStatus();

  boolean isTerminated();

  List<Task> getTasks();

  Job getParentJob();

  String getId();

  String getInfo();

  int getPriority();

  JobConfiguration getJobConfiguration();

  Long getCreationTime();

  Long getLastModificationTime();

  Long getStartTime();

  Long getEndTime();

  boolean hasSubJob();

  boolean reset(boolean force) throws Exception;

  boolean retry();

  void setInfo(String info);

  void setStatus(Task task);

  void stop() throws Exception;

  void update(JobConfiguration jobConfiguration);
}
