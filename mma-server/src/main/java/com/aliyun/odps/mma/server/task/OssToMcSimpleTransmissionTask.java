package com.aliyun.odps.mma.server.task;

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.OssToMcSimpleTransmissionAction;
import com.aliyun.odps.mma.server.job.Job;

public class OssToMcSimpleTransmissionTask extends DagTask {

  private final Job job;
  private final OssConfig ossConfig;
  private final ObjectType objectType;

  public OssToMcSimpleTransmissionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      OssConfig ossConfig,
      ObjectType objectType,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.ossConfig = ossConfig;
    this.objectType = objectType;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);

    Odps odps = OdpsUtils.getOdps(
        config.get(AbstractConfiguration.METADATA_DEST_MC_ACCESS_KEY_ID),
        config.get(AbstractConfiguration.METADATA_DEST_MC_ACCESS_KEY_SECRET),
        config.get(AbstractConfiguration.METADATA_DEST_MC_ENDPOINT),
        config.get(JobConfiguration.DEST_CATALOG_NAME)
    );

    String[] fileNames = OssUtils.getOssPaths(
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_PATH),
        rootJobId,
        config.get(JobConfiguration.OBJECT_TYPE),
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME));
    String metafile = fileNames[0];
    String datafile = fileNames[1] + config.get(JobConfiguration.SOURCE_OBJECT_NAME);

    OssToMcSimpleTransmissionAction action = new OssToMcSimpleTransmissionAction(
        id + ".Transmission",
        ossConfig,
        objectType,
        metafile,
        datafile,
        odps,
        true,
        this,
        context);
    dag.addVertex(action);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
