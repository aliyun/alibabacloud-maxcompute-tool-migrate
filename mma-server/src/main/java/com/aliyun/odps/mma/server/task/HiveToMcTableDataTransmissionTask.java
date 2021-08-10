package com.aliyun.odps.mma.server.task;

import java.util.List;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.HiveToMcTableDataTransmissionAction;
import com.aliyun.odps.mma.server.action.HiveVerificationAction;
import com.aliyun.odps.mma.server.action.McVerificationAction;
import com.aliyun.odps.mma.server.action.VerificationAction;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class HiveToMcTableDataTransmissionTask extends TableDataTransmissionTask {

  public HiveToMcTableDataTransmissionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel hiveTableMetaModel,
      TableMetaModel mcTableMetaModel,
      Job job,
      List<Job> subJobs) {
    super(id, rootJobId, config, hiveTableMetaModel, mcTableMetaModel, job, subJobs);
    init();
  }

  private void init() {
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.DEST_CATALOG_NAME));
    ActionExecutionContext context = new ActionExecutionContext(config);
    HiveToMcTableDataTransmissionAction dataTransmissionAction =
        new HiveToMcTableDataTransmissionAction(
            id + ".DataTransmission",
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_URL),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_USERNAME),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_PASSWORD),
            source,
            dest,
            this,
            context);
    dag.addVertex(dataTransmissionAction);

    HiveVerificationAction hiveVerificationAction = new HiveVerificationAction(
        id + ".HiveDataVerification",
        config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_URL),
        config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_USERNAME),
        config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_PASSWORD),
        source,
        true,
        this,
        context);
    dag.addVertex(hiveVerificationAction);

    McVerificationAction mcVerificationAction = new McVerificationAction(
        id + ".McDataVerification",
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
        executionProject,
        config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
        dest,
        false,
        this,
        context);
    dag.addVertex(mcVerificationAction);

    VerificationAction verificationAction = new VerificationAction(
        id + ".FinalVerification",
        source,
        this,
        context);
    dag.addVertex(verificationAction);

    dag.addEdge(dataTransmissionAction, hiveVerificationAction);
    dag.addEdge(dataTransmissionAction, mcVerificationAction);
    dag.addEdge(hiveVerificationAction, verificationAction);
    dag.addEdge(mcVerificationAction, verificationAction);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
