package com.aliyun.odps.mma.server.job;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.task.Task;

public class OssToMcCatalogJob extends CatalogJob {
  private static final Logger LOG = LogManager.getLogger(OssToMcCatalogJob.class);

  public OssToMcCatalogJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public synchronized List<Task> getExecutableTasks() {
    List<Job> subJobs = getSubJobs();
    List<Task> ret = new LinkedList<>();

    // Firstly, execute table jobs
    List<Job> nonSucceededJobTable = subJobs
        .stream()
        .filter(j -> ObjectType.TABLE.name().equals(j.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE)))
        .filter(j -> !JobStatus.SUCCEEDED.equals(j.getStatus()))
        .collect(Collectors.toList());
    if (!nonSucceededJobTable.isEmpty()) {
      for (Job j : nonSucceededJobTable) {
        ret.addAll(j.getExecutableTasks());
      }
      return ret;
    }

    // Then, execute resource jobs
    // Finally, execute function jobs
    return ret;
  }
}
