package com.aliyun.odps.mma.server.task;

import java.util.List;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public abstract class TableDataTransmissionTask extends DagTask {

  TableMetaModel source;
  TableMetaModel dest;
  Job job;
  List<Job> subJobs;

  public TableDataTransmissionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel source,
      TableMetaModel dest,
      Job job,
      List<Job> subJobs) {
    super(id, rootJobId, config);
    this.source = source;
    this.dest = dest;
    this.job = job;
    this.subJobs = subJobs;
  }

  public List<Job> getSubJobs() {
    return subJobs;
  }
}
