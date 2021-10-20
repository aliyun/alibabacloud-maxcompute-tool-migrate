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

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.task.Task;

/**
 * mc -> oss & oss -> mc
 */
public class McToOssCatalogJob extends CatalogJob {

  private static final Logger LOG = LogManager.getLogger(McToOssCatalogJob.class);

  public McToOssCatalogJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public synchronized List<Task> getExecutableTasks() {
    List<Job> subJobs = getSubJobs();
    List<Task> ret = new LinkedList<>();

    // Firstly, execute table jobs
    getJobsByObjectType(ret, subJobs, ObjectType.TABLE);
    if (!ret.isEmpty()) {
      return ret;
    }

    // Then, execute resource jobs
    getJobsByObjectType(ret, subJobs, ObjectType.RESOURCE);
    if (!ret.isEmpty()) {
      return ret;
    }

    // Finally, execute function jobs
    getJobsByObjectType(ret, subJobs, ObjectType.FUNCTION);
    return ret;
  }


  void getJobsByObjectType(List<Task> ret, List<Job> subJobs, ObjectType objectType) {
    List<Job> nonSucceededJob = subJobs
        .stream()
        .filter(j -> objectType.name()
            .equals(j.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE)))
        .filter(j -> !JobStatus.SUCCEEDED.equals(j.getStatus()))
        .collect(Collectors.toList());
    for (Job job : nonSucceededJob) {
      String debug = config.getOrDefault(AbstractConfiguration.DEBUG_MODE,
                                         AbstractConfiguration.DEBUG_MODE_DEFAULT_VALUE);
      if (Boolean.parseBoolean(debug)) {

      }
      ret.addAll(job.getExecutableTasks());
    }
  }
}
