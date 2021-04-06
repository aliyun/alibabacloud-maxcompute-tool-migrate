package com.aliyun.odps.mma.server.task;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McToOssTableMetadataTransmissionAction;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class McToOssTableMetadataTransmissionTask extends DagTask {

  private TableMetaModel tableMetaModel;
  private Job job;

  public McToOssTableMetadataTransmissionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel tableMetaModel,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.tableMetaModel = tableMetaModel;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    McToOssTableMetadataTransmissionAction action = new McToOssTableMetadataTransmissionAction(
        id + ".MetadataTransmission",
        tableMetaModel,
        config.get(JobConfiguration.DATA_DEST_OSS_ACCESS_KEY_ID),
        config.get(JobConfiguration.DATA_DEST_OSS_ACCESS_KEY_SECRET),
        config.get(JobConfiguration.DATA_DEST_OSS_ROLE_ARN),
        config.get(JobConfiguration.DATA_DEST_OSS_BUCKET),
        config.get(JobConfiguration.DATA_DEST_OSS_ENDPOINT),
        this,
        context);
    dag.addVertex(action);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }

  @Override
  public String getJobId() {
    // TODO: remove
    return null;
  }
}
