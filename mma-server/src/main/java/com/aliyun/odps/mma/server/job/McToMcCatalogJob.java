package com.aliyun.odps.mma.server.job;

import java.util.List;
import java.util.LinkedList;
import java.util.stream.Collectors;

import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.Task;

public class McToMcCatalogJob extends CatalogJob {
  private static final Logger LOG = LogManager.getLogger(McToMcCatalogJob.class);

  public McToMcCatalogJob(Job parentJob,
                          JobRecord record,
                          JobManager jobManager,
                          MetaManager metaManager,
                          MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public synchronized List<Task> getExecutableTasks() {
    LOG.info("Create the Mc2Mc catalogJob, job id: {}", record.getJobId());
    List<Job> subJobs = getSubJobs();
    List<Task> ret = new LinkedList<>();

    // Step.1: sync table jobs.
    getJobsByObjectType(ret, subJobs, ObjectType.TABLE);
    if (!ret.isEmpty()) {
      LOG.info("The first step is to sync the table jobs: {}", ret);
      return ret;
    }
    if (!subJobs.stream()
            .filter(subJob -> ObjectType.TABLE.name().equals(subJob.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE)))
            .allMatch(subJob -> JobStatus.SUCCEEDED.equals(subJob.getStatus()))) {
      LOG.info("The subjobs of ObjectType.TABLE from McToMcCatalogJob aren't finished yet.");
      return ret;
    }

    // Step.2: sync resource jobs.
    getJobsByObjectType(ret, subJobs, ObjectType.RESOURCE);
    if (!ret.isEmpty()) {
      LOG.info("The second step is to sync the resource jobs: {}", ret);
      return ret;
    }

    if (!subJobs.stream()
            .filter(subJob -> ObjectType.RESOURCE.name().equals(subJob.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE)))
            .allMatch(subJob -> JobStatus.SUCCEEDED.equals(subJob.getStatus()))) {
      LOG.info("The subjobs of ObjectType.RESOURCE from McToMcCatalogJob aren't finished yet.");
      return ret;
    }

    // Step.3: sync function jobs.
    getJobsByObjectType(ret, subJobs, ObjectType.FUNCTION);
    LOG.info("The third step is to sync the functions jobs: {}", ret);
    return ret;
  }


  void getJobsByObjectType(List<Task> ret, List<Job> subJobs, ObjectType objectType) {
    List<Job> unSucceededJob = subJobs.stream()
            .filter(subJob -> objectType.name().equals(subJob.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE))
                    && !JobStatus.SUCCEEDED.equals(subJob.getStatus()))
            .collect(Collectors.toList());
    for (Job job : unSucceededJob) {
      ret.addAll(job.getExecutableTasks());
    }
  }
}
