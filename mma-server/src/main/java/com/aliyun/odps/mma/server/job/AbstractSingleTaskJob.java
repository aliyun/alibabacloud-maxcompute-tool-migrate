package com.aliyun.odps.mma.server.job;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;

/**
 * @author yida
 */
public abstract class AbstractSingleTaskJob extends AbstractJob {

  private static final Logger LOG = LogManager.getLogger(AbstractSingleTaskJob.class);
  private Task task;

  public AbstractSingleTaskJob(
      Job parentJob,
      JobRecord record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public List<Task> getExecutableTasks() {
    if (task == null) {
      try {
        task = generateTask();
      } catch (Exception e) {
        fail("Task generate fail");
        return Collections.emptyList();
      }
      if (task == null) {
        fail("Task is null");
        return Collections.emptyList();
      }
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

  @Override
  public synchronized boolean retry() {
    boolean retry = super.retry();
    if (retry) {
      try {
        LOG.info( "Retry job, id: {}, regenerate task", record.getJobId());
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
      return;
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
