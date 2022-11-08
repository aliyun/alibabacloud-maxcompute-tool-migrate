package com.aliyun.odps.mma.server.action;

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.resource.Resource;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class McToMcTableDataTransmissionAction extends CopyTaskAction {
  private final String srcProject;
  private final String srcOdpsAccessId;
  private final String srcOdpsAccessKey;
  private final String srcOdpsEndpoint;
  private final String destProject;
  private final String destOdpsAccessId;
  private final String destOdpsAccessKey;
  private final String destOdpsEndpoint;


  public McToMcTableDataTransmissionAction(
          String id,
          TableMetaModel source,
          TableMetaModel dest,
          String srcProject,
          String srcOdpsAccessId,
          String srcOdpsAccessKey,
          String srcOdpsEndpoint,
          String destProject,
          String destOdpsAccessId,
          String destOdpsAccessKey,
          String destOdpsEndpoint,
          Task task, ActionExecutionContext context) {
    super(id, source, dest, task, context);
    this.srcProject = srcProject;
    this.srcOdpsAccessId = srcOdpsAccessId;
    this.srcOdpsAccessKey = srcOdpsAccessKey;
    this.srcOdpsEndpoint = srcOdpsEndpoint;
    this.destProject = destProject;
    this.destOdpsAccessId = destOdpsAccessId;
    this.destOdpsAccessKey = destOdpsAccessKey;
    this.destOdpsEndpoint = destOdpsEndpoint;

    JobConfiguration config = actionExecutionContext.getConfig();
    MmaServerConfiguration mmaServerConfiguration = MmaServerConfiguration.getInstance();
    long numDataWorkerResource = Long.parseLong(
            config.getOrDefault(
                    JobConfiguration.JOB_NUM_DATA_WORKER,
                    mmaServerConfiguration.getOrDefault(
                            AbstractConfiguration.JOB_NUM_DATA_WORKER,
                            AbstractConfiguration.JOB_NUM_DATA_WORKER_DEFAULT_VALUE)
            )
    );

    resourceMap.put(Resource.DATA_WORKER, numDataWorkerResource);

  }

  @Override
  public String getName() {
    return "Table data transmission";
  }

  @Override
  public Odps getSrcOdps() {
    return OdpsUtils.getOdps(
            this.srcOdpsAccessId,
            this.srcOdpsAccessKey,
            this.srcOdpsEndpoint,
            this.srcProject);
  }

  @Override
  public Odps getDestOdps() {
    return OdpsUtils.getOdps(
            this.destOdpsAccessId,
            this.destOdpsAccessKey,
            this.destOdpsEndpoint,
            this.destProject);
  }

}
