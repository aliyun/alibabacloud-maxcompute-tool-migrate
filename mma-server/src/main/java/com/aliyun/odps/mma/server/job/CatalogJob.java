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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.job.utils.JobUtils;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.Task;

public class CatalogJob extends AbstractJob {

  private static final Logger LOG = LogManager.getLogger(CatalogJob.class);

  private static final int EXECUTABLE_TASK_BATCH_SIZE = 3;

  public CatalogJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public boolean plan() {
    //TODO refactor
    boolean success = false;
    try {
      if ("1".equals(config.get(JobConfiguration.PLAN_INIT))) {
        LOG.info("PLANed" + config.get(JobConfiguration.PLAN_INIT));
        return true;
      }
      LOG.info("PLAN" + config.get(JobConfiguration.PLAN_INIT));
      jobManager.addCatalogJob(record);

      Map<String, String> newConfig = new HashMap<>(config);
      newConfig.put(JobConfiguration.PLAN_INIT, "1");
      config = new JobConfiguration(newConfig);
      success = true;
    } catch (Exception e) {
      LOG.info(ExceptionUtils.getStackTrace(e));
      setStatusInternal(JobStatus.FAILED);
      for (Job job: getSubJobs()) {
        try {
          //TODO status => fail
          job.stop();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    } finally {
      com.aliyun.odps.mma.server.meta.generated.Job.JobBuilder
          jobBuilder = new com.aliyun.odps.mma.server.meta.generated.Job.JobBuilder(record);
      jobBuilder.attemptTimes(record.getAttemptTimes()+1);
      update(jobBuilder);
    }

    return success;
  }

  @Override
  public synchronized JobStatus getStatus() {
    JobStatus jobStatus = JobStatus.valueOf(record.getJobStatus());

    if (JobStatus.FAILED.equals(jobStatus) || JobStatus.CANCELED.equals(jobStatus)) {
      return jobStatus;
    }

    JobStatus aggregatedJobStatus = JobUtils.getJobStatus(record, metaManager);
    if (!aggregatedJobStatus.equals(jobStatus)) {
      // Correct job status
      LOG.info(
          "Current job status: {}, aggregated job status: {}",
          jobStatus,
          aggregatedJobStatus);
      setStatusInternal(aggregatedJobStatus);
    }
    return jobStatus;
  }

  @Override
  public List<Task> getExecutableTasks() {
    // Iterate over sub jobs and pick up executable tasks
    List<Job> subJobs = getSubJobs();
    List<Task> ret = new ArrayList<>(EXECUTABLE_TASK_BATCH_SIZE);
    for (Job subJob : subJobs) {
      ret.addAll(subJob.getExecutableTasks());
      if (ret.size() > EXECUTABLE_TASK_BATCH_SIZE) {
        // Make this method call return once a few executable tasks are generated, so that this
        // method call won't block the execution.
        break;
      }
    }
    return ret;
  }

  @Override
  boolean updateObjectMetadata() {
    return false;
  }
}
