package com.aliyun.odps.mma.server.job;

import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.McToMcResourceTask;
import com.aliyun.odps.mma.server.task.TaskProgress;

public class McToMcResourceJob extends AbstractSingleTaskJob {
  private static final Logger LOG = LogManager.getLogger(McToMcResourceJob.class);
  private Task task;

  public McToMcResourceJob(Job parentJob,
                           JobRecord record,
                           JobManager jobManager,
                           MetaManager metaManager,
                           MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  Task generateTask() {
    String taskIdPrefix = generateTaskIdPrefix();
    this.task = new McToMcResourceTask(
            taskIdPrefix + ".resourceTransmission",
            getRootJobId(),
            config,
            this
    );
    return task;
  }

  @Override
  public synchronized void setStatus(Task task) {
    if (this.task != task) {
      LOG.info("Outdated task(McToMcResource) found, job id: {}, task id：{}", record.getJobId(), task.getId());
      return;
    }

    if (this.isTerminated()) {
      LOG.info("Job(McToMcResource) has terminated, id: {}, status: {}, task id: {}, task status: {}",
              record.getJobId(),
              getStatus(),
              task.getId(),
              task.getProgress());
    }

    TaskProgress taskStatus = task.getProgress();

    switch (taskStatus) {
      case SUCCEEDED:
        setStatusInternal(JobStatus.SUCCEEDED);
        setInfo(Lists.newArrayList(task.getDag().iterator()).get(0).getResult().toString());
        break;
      case FAILED:
        setStatusInternal(JobStatus.FAILED);
        fail(Lists.newArrayList(task.getDag().iterator()).get(0).getResult().toString());
        break;
      case RUNNING:
        setStatusInternal(JobStatus.RUNNING);
        break;
      default:
        break;
    }
  }
}
