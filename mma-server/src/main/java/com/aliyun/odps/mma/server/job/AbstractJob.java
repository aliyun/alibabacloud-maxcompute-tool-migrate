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
import java.util.Objects;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.generated.Job.JobBuilder;
import com.aliyun.odps.mma.server.task.Task;

public abstract class AbstractJob implements Job {
  private static final Logger LOG = LogManager.getLogger(AbstractJob.class);

  Job parentJob;
  JobConfiguration config;
  com.aliyun.odps.mma.server.meta.generated.Job record;

  JobManager jobManager;
  MetaManager metaManager;
  MetaSourceFactory metaSourceFactory;

  AbstractJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    this.parentJob = parentJob;
    this.record = Objects.requireNonNull(record);
    this.config = JobConfiguration.fromJson(record.getJobConfig());
    this.jobManager = Objects.requireNonNull(jobManager);
    this.metaManager = Objects.requireNonNull(metaManager);
    this.metaSourceFactory = Objects.requireNonNull(metaSourceFactory);
  }

  @Override
  public void init() {

  }

  @Override
  public List<Task> getTasks() {
    return Collections.emptyList();
  }

  @Override
  public boolean hasSubJob() {
    return record.hasSubJob();
  }

  @Override
  public List<Job> getSubJobs() {
    return jobManager.listSubJobs(this);
  }

  @Override
  public JobStatus getStatus() {
    return JobStatus.valueOf(record.getJobStatus());
  }

  @Override
  public Job getParentJob() {
    return parentJob;
  }

  @Override
  public String getId() {
    return record.getJobId();
  }

  @Override
  public int getPriority() {
    return record.getJobPriority();
  }

  @Override
  public String getInfo() {
    return record.getJobInfo();
  }

  @Override
  public Long getCreationTime() {
    return record.getCTime();
  }

  @Override
  public Long getLastModificationTime() {
    return record.getMTime();
  }

  @Override
  public Long getStartTime() {
    return record.getSTime();
  }

  @Override
  public Long getEndTime() {
    return record.getETime();
  }

  @Override
  public JobConfiguration getJobConfiguration() {
    return config;
  }

  @Override
  public synchronized void setInfo(String info) {
    JobBuilder jobBuilder = new JobBuilder(record);
    jobBuilder.jobInfo(info);
    update(jobBuilder);
  }

  @Override
  public synchronized void setStatus(Task task) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean reset(boolean force) throws Exception {
    LOG.info("Reset job, job id: {}", record.getJobId());

    JobStatus status = JobStatus.valueOf(record.getJobStatus());

    if (isTerminated()) {
      boolean needReset = false;
      if (hasSubJob()) {
        removeInvalidSubJobs();
        for (Job j : getSubJobs()) {
          needReset = j.reset(force) || needReset;
        }
        needReset = addNewSubJobs() || needReset;
      }

      boolean objectChanged = updateObjectMetadata();
      if (objectChanged) {
        LOG.info("Object has changed, job id: {}", record.getJobId());
      }

      if (force
          || needReset
          || JobStatus.FAILED.equals(status)
          || JobStatus.CANCELED.equals(status)
          || objectChanged) {
        JobBuilder jobBuilder = new JobBuilder(record);
        jobBuilder.jobStatus(JobStatus.PENDING);
        jobBuilder.attemptTimes(0);
        jobBuilder.sTime(System.currentTimeMillis());
        jobBuilder.jobInfo("");
        update(jobBuilder);
        LOG.info(
            "Job has been reset, job id: {}, prev status: {}, curr status: {}",
            record.getJobId(),
            status,
            record.getJobStatus());
        return true;
      } else {
        LOG.info("No need to reset, job id: {}", record.getJobId());
        return false;
      }
    } else {
      LOG.warn("Unable to reset a pending or running job, id: {}", record.getJobId());
      throw new IllegalStateException(
          "Unable to reset a pending or running job, id: " + record.getJobId());
    }
  }

  /**
   * Update the source object's metadata.
   *
   * @return Return true if the source object has been changed, false else.
   */
  abstract boolean updateObjectMetadata() throws Exception;

  /**
   * Remove sub jobs whose source object doesn't exists anymore.
   */
  void removeInvalidSubJobs() throws Exception {
  }

  /**
   * Add new sub jobs.
   *
   * @return Return true if there are new sub jobs, indicating the parent job should be reset.
   */
  boolean addNewSubJobs() throws Exception {
    return false;
  }

  @Override
  public synchronized boolean retry() {
    if (record.getAttemptTimes() < record.getMaxAttemptTimes()) {
      LOG.info(
          "Retry job, id: {}, attempt times: {}, max: {}",
          record.getJobId(),
          record.getAttemptTimes(),
          record.getMaxAttemptTimes());
      if (record.hasSubJob()) {
        for (Job j : getSubJobs()) {
          if (JobStatus.FAILED.equals(j.getStatus()) || JobStatus.CANCELED.equals(j.getStatus())) {
            if (!j.retry()) {
              LOG.info(
                  "Unable to retry job because of one of its sub job, id: {}, sub job id: {}",
                  record.getJobId(),
                  j.getId());
            }
          }
        }
      }
      JobBuilder jobBuilder = new JobBuilder(record);
      jobBuilder.jobStatus(JobStatus.PENDING);
      jobBuilder.sTime(System.currentTimeMillis());
      jobBuilder.jobInfo("");
      update(jobBuilder);

      return true;
    } else {
      LOG.info(
          "Reach max attempt times, job id: {}, attempt times: {}, max: {}",
          record.getJobId(),
          record.getAttemptTimes(),
          record.getMaxAttemptTimes());
      return false;
    }
  }

  @Override
  public synchronized void stop() throws MmaException {
    if (isTerminated()) {
      LOG.info("Stop terminated job, job id: {}", record.getJobId());
    } else {
      LOG.info("Stop job, job id: {}, status: {}", record.getJobId(), record.getJobStatus());
      setStatusInternal(JobStatus.CANCELED);
    }
    // The job status may be inconsistent because of unexpected process termination. Thus, we need
    // to traverse the job tree and try to stop all the sub jobs even when this job itself is
    // already terminated.
    if (record.hasSubJob()) {
      List<Job> subJobs = jobManager.listSubJobs(this);
      for (Job subJob : subJobs) {
        ((AbstractJob) subJob).stop();
      }
    }
  }

  @Override
  public String toString() {
    String objectTypeStr = config.get(JobConfiguration.OBJECT_TYPE);
    String metaSourceTypeStr = config.get(JobConfiguration.METADATA_SOURCE_TYPE);
    String dataSourceTypeStr = config.get(JobConfiguration.DATA_SOURCE_TYPE);
    String metaDestTypeStr = config.get(JobConfiguration.METADATA_DEST_TYPE);
    String dataDestTypeStr = config.get(JobConfiguration.DATA_DEST_TYPE);
    String sourceCatalog = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String sourceObject = config.get(JobConfiguration.SOURCE_OBJECT_NAME);

    return String.join(
        ", ",
        record.getJobId(),
        objectTypeStr,
        metaSourceTypeStr,
        dataSourceTypeStr,
        metaDestTypeStr,
        dataDestTypeStr,
        sourceCatalog,
        sourceObject,
        record.getJobStatus());
  }

  @Override
  public void update(JobConfiguration jobConfiguration) {
  }

  @Override
  public boolean clean() {
    return true;
  }

  /**
   * Helper methods for sub classes
   */
  void setStatusInternal(JobStatus status) {
    if (record.getJobStatus().equals(status.name())) {
      return;
    }

    LOG.info(
        "Set job status, id: {}, from: {}, to: {}",
        record.getJobId(),
        record.getJobStatus(),
        status);

    long time = System.currentTimeMillis();
    JobBuilder jobBuilder = new JobBuilder(record);
    jobBuilder.jobStatus(status);
    switch (status) {
      case CANCELED:
      case SUCCEEDED:
      case FAILED:
        jobBuilder.eTime(time);
        break;
      case RUNNING:
        jobBuilder.sTime(time);
        jobBuilder.attemptTimes(record.getAttemptTimes() + 1);
        break;
      default:
    }

    update(jobBuilder);
  }

  void fail(String reason) {
    LOG.info("Job failed, id: {}, reason: {}", record.getJobId(), reason);
    setStatusInternal(JobStatus.FAILED);
    setInfo(reason);
  }

  void reload() {
    if (parentJob != null) {
      record = metaManager.getSubJobById(parentJob.getId(), record.getJobId());
    } else {
      record = metaManager.getJobById(record.getJobId());
    }
  }

  void update(JobBuilder jobBuilder) {
    reload();
    if (parentJob != null) {
      metaManager.updateSubJobById(
          parentJob.getId(),
          com.aliyun.odps.mma.server.meta.generated.Job.from(jobBuilder));
    } else {
      metaManager.updateJobById(
          com.aliyun.odps.mma.server.meta.generated.Job.from(jobBuilder));
    }
    reload();
  }

  boolean isTerminated() {
    JobStatus status = getStatus();
    return JobStatus.SUCCEEDED.equals(status)
        || JobStatus.FAILED.equals(status)
        || JobStatus.CANCELED.equals(status);
  }

  String generateTaskIdPrefix() {
    return UUID.randomUUID().toString();
  }

  String getRootJobId() {
    if (parentJob == null) {
      return getId();
    }

    Job currentJob = this;
    while (currentJob.getParentJob() != null) {
      currentJob = getParentJob();
    }
    return currentJob.getId();
  }

}
