package com.aliyun.odps.mma.server.task;

import java.util.List;

import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McToMcTableDataTransmissionAction;

public class McToMcTableDataTransmissionTask extends TableDataTransmissionTask {

  public McToMcTableDataTransmissionTask(
          String id,
          String rootJobId,
          JobConfiguration config,
          TableMetaModel source,
          TableMetaModel dest,
          Job job,
          List<Job> subJobs) {
    super(id, rootJobId, config, source, dest, job, subJobs);
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);

    McToMcTableDataTransmissionAction mcToMcTableDataTransmissionAction =
            new McToMcTableDataTransmissionAction(
                    id + ".DataTransmission",
                    source,
                    dest,
                    config.get(JobConfiguration.SOURCE_CATALOG_NAME),
                    config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_ID),
                    config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_SECRET),
                    config.get(JobConfiguration.DATA_SOURCE_MC_ENDPOINT),
                    config.get(JobConfiguration.DEST_CATALOG_NAME),
                    config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
                    config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
                    config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
                    this,
                    context
    );

    dag.addVertex(mcToMcTableDataTransmissionAction);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
