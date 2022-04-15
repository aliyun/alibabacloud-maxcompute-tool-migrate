package com.aliyun.odps.mma.server.job;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import com.aliyun.odps.mma.server.task.McToMcFunctionTask;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class McToMcFunctionJob extends AbstractSingleTaskJob {
  private static final Logger LOG = LogManager.getLogger(McToMcFunctionJob.class);

  private Task task;

  public McToMcFunctionJob(Job parentJob,
                           JobRecord record,
                           JobManager jobManager,
                           MetaManager metaManager,
                           MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  Task generateTask() {
    String taskIdPrefix = generateTaskIdPrefix();
    this.task = new McToMcFunctionTask(
            taskIdPrefix + ".functionTransmission",
            getRootJobId(),
            config,
            this
    );
    return task;
  }

  @Override
  public synchronized void setStatus(Task task) {
    if (this.task != task) {
      LOG.info("Outdated task(McToMcFunction) found, job id: {}, task id：{}",
              record.getJobId(),
              task.getId());
      return;
    }

    if (this.isTerminated()) {
      LOG.info("Job(McToMcFunction) has terminated, id: {}, status: {}, task id: {}, task status: {}",
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
