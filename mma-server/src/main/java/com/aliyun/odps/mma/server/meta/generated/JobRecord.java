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

package com.aliyun.odps.mma.server.meta.generated;

import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.lang3.Validate;

import com.aliyun.odps.mma.job.JobStatus;
import lombok.Data;

@Data
public class JobRecord implements Serializable {

  public static class JobBuilder {

    private String jobId;
    private Integer jobPriority;
    private String jobStatus;
    private String jobConfig;
    private Integer attemptTimes;
    private Integer maxAttemptTimes;
    private Long cTime;
    private Long mTime;
    private Long sTime;
    private Long eTime;
    private Boolean hasSubJob;
    private String jobInfo;

    public JobBuilder(JobRecord jobRecord) {
      this.jobId = jobRecord.jobId;
      this.jobPriority = jobRecord.jobPriority;
      this.jobStatus = jobRecord.jobStatus;
      this.jobConfig = jobRecord.jobConfig;
      this.attemptTimes = jobRecord.attemptTimes;
      this.maxAttemptTimes = jobRecord.maxAttemptTimes;
      this.cTime = jobRecord.cTime;
      this.mTime = jobRecord.mTime;
      this.sTime = jobRecord.sTime;
      this.eTime = jobRecord.eTime;
      this.hasSubJob = jobRecord.hasSubJob;
      this.jobInfo = jobRecord.jobInfo;
    }

    public JobBuilder jobStatus(JobStatus jobStatus) {
      this.jobStatus = jobStatus.name();
      return this;
    }

    public JobBuilder attemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
      return this;
    }

    public JobBuilder sTime(long sTime) {
      this.sTime = sTime;
      return this;
    }

    public JobBuilder eTime(long eTime) {
      this.eTime = eTime;
      return this;
    }

    public JobBuilder jobInfo(String jobInfo) {
      this.jobInfo = jobInfo;
      return this;
    }

    public JobBuilder jobConfig(String jobConfig) {
      this.jobConfig = Validate.notNull(jobConfig);
      return this;
    }
  }

  private String jobId;
  private Integer jobPriority;
  private String jobStatus;
  private String jobConfig;
  private Integer attemptTimes;
  private Integer maxAttemptTimes;
  private Long cTime;
  private Long mTime;
  private Long sTime;
  private Long eTime;
  private Boolean hasSubJob;
  private String jobInfo;

  private static final long serialVersionUID = 1L;

  public String getJobId() {
    return jobId;
  }

  public Integer getJobPriority() {
    return jobPriority;
  }

  public String getJobStatus() {
    return jobStatus;
  }

  public String getJobConfig() {
    return jobConfig;
  }

  public Integer getAttemptTimes() {
    return attemptTimes;
  }

  public Integer getMaxAttemptTimes() {
    return maxAttemptTimes;
  }

  public Long getCTime() {
    return cTime;
  }

  public Long getMTime() {
    return mTime;
  }

  public Long getSTime() {
    return sTime;
  }

  public Long getETime() {
    return eTime;
  }

  public boolean hasSubJob() {
    return hasSubJob;
  }

  public String getJobInfo() {
    return jobInfo;
  }

  public static JobRecord of(
      String jobId,
      int jobPriority,
      String jobStatus,
      String jobConfig,
      int attemptTimes,
      int maxAttemptTimes,
      long cTime,
      long mTime,
      long sTime,
      long eTime,
      boolean hasSubJob,
      String jobInfo) {
    JobRecord record = new JobRecord();
    record.jobId = Objects.requireNonNull(jobId);
    record.jobPriority = jobPriority;
    record.jobStatus = Objects.requireNonNull(jobStatus);
    record.jobConfig = Objects.requireNonNull(jobConfig);
    record.attemptTimes = attemptTimes;
    record.maxAttemptTimes = maxAttemptTimes;
    record.cTime = cTime;
    record.mTime = mTime;
    record.sTime = sTime;
    record.eTime = eTime;
    record.hasSubJob = hasSubJob;
    record.jobInfo = jobInfo;
    return record;
  }

  public static JobRecord from(JobBuilder jobBuilder) {
    JobRecord record = new JobRecord();
    record.jobId = jobBuilder.jobId;
    record.jobPriority = jobBuilder.jobPriority;
    record.jobStatus = jobBuilder.jobStatus;
    record.jobConfig = jobBuilder.jobConfig;
    record.attemptTimes = jobBuilder.attemptTimes;
    record.maxAttemptTimes = jobBuilder.maxAttemptTimes;
    record.cTime = jobBuilder.cTime;
    record.mTime = jobBuilder.mTime;
    record.sTime = jobBuilder.sTime;
    record.eTime = jobBuilder.eTime;
    record.hasSubJob = jobBuilder.hasSubJob;
    record.jobInfo = jobBuilder.jobInfo;
    return record;
  }
}