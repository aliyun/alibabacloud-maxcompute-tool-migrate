package com.aliyun.odps.mma.server.job;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;

/**
 * @author yida
 */
public abstract class SimpleTransmissionJob extends AbstractJob{

  private static final Logger LOG = LogManager.getLogger(SimpleTransmissionJob.class);
  private Task task;

  public SimpleTransmissionJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public List<Task> getExecutableTasks() {
    if (task == null && JobStatus.PENDING.equals(getStatus())) {
      task = generateTask();
    }
    if (task == null) {
      // Exception happened when generating the DAG.
      LOG.info("job {} has not tasks");
      return Collections.emptyList();
    }

    if (TaskProgress.PENDING.equals(task.getProgress())) {
      return Collections.singletonList(task);
    } else {
      return Collections.emptyList();
    }
  }

  abstract Task generateTask();

  @Override
  boolean updateObjectMetadata() throws Exception {
    // todo
    return false;
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

  @Override
  public synchronized boolean retry() {
    boolean retry = super.retry();
    if (retry) {
      try {
        task = generateTask();
      } catch (Exception e) {
        return false;
      }
    }
    return retry;
  }

  @Override
  public synchronized void setStatus(Task task) {
    if (this.task != task) {
      LOG.info("Outdated task found, job id: {}, task id：{}", record.getJobId(), task.getId());
      return;
    }

    if (this.isTerminated()) {
      LOG.info("Job has terminated, id: {}, status: {}, task id: {}, task status: {}",
               record.getJobId(),
               getStatus(),
               task.getId(),
               task.getProgress());
    }

    TaskProgress taskStatus = task.getProgress();

    switch (taskStatus) {
      case SUCCEEDED:
        setStatusInternal(JobStatus.SUCCEEDED);
        break;
      case FAILED:
        setStatusInternal(JobStatus.FAILED);
        break;
      case RUNNING:
        setStatusInternal(JobStatus.RUNNING);
        break;
      default:
        break;
    }
  }
}
