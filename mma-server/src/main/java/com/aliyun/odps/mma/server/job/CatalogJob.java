package com.aliyun.odps.mma.server.job;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.job.utils.JobUtils;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.Task;

public class CatalogJob extends AbstractJob {

  private static final Logger LOG = LogManager.getLogger(CatalogJob.class);

  public CatalogJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
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
    List<Task> ret = new LinkedList<>();
    for (Job subJob : subJobs) {
      ret.addAll(subJob.getExecutableTasks());
    }
    return ret;
  }

  @Override
  boolean updateObjectMetadata() {
    return false;
  }
}
