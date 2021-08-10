package com.aliyun.odps.mma.server.task;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McDropTableAction;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class OssToMcTableCleanUpTask extends DagTask {
  private TableMetaModel tableMetaModel;
  private Job job;

  public OssToMcTableCleanUpTask (
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel tableMetaModel,
      Job job) {
    super(id, rootJobId, config);
    this.tableMetaModel = tableMetaModel;
    this.job = job;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.DEST_CATALOG_NAME));
    McDropTableAction action = new McDropTableAction(
        id + ".DropExternalTable",
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
        executionProject,
        config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
        tableMetaModel,
        this,
        context);
    dag.addVertex(action);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}