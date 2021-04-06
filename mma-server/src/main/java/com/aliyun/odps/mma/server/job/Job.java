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

  List<Task> getExecutableTasks();

  List<Job> getSubJobs();

  JobStatus getStatus();

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
