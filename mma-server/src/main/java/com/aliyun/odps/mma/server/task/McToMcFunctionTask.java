package com.aliyun.odps.mma.server.task;

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McToMcFunctionAction;
import com.aliyun.odps.mma.server.job.Job;

public class McToMcFunctionTask extends DagTask {
  private final Job job;

  public McToMcFunctionTask(String id, String rootJobId, JobConfiguration config, Job job) {
    super(id, rootJobId, config);
    this.job = job;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    Odps srcOdps = OdpsUtils.getOdps(
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_SECRET),
            config.get(JobConfiguration.DATA_SOURCE_MC_ENDPOINT),
            config.get(JobConfiguration.SOURCE_CATALOG_NAME)
    );
    Odps destOdps = OdpsUtils.getOdps(
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            config.get(JobConfiguration.DEST_CATALOG_NAME)
    );

    McToMcFunctionAction action = new McToMcFunctionAction(
            id,
            this,
            context,
            config,
            srcOdps,
            destOdps
    );
    dag.addVertex(action);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
