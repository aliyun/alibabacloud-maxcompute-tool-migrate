package com.aliyun.odps.mma.server.task;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McAddPartitionsAction;
import com.aliyun.odps.mma.server.action.McCreateOssExternalTableAction;
import com.aliyun.odps.mma.server.action.McCreateTableAction;
import com.aliyun.odps.mma.server.job.AbstractTableJob.TablePartitionGroup;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class OssToMcTableSetUpTask extends DagTask {
  private TableMetaModel ossTableMetaModel;
  private TableMetaModel mcTableMetaModel;
  private List<TablePartitionGroup> partitionGroups;
  private Job job;

  public OssToMcTableSetUpTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel ossTableMetaModel,
      TableMetaModel mcTableMetaModel,
      List<TablePartitionGroup> partitionGroups,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.ossTableMetaModel = ossTableMetaModel;
    this.mcTableMetaModel = mcTableMetaModel;
    this.partitionGroups = partitionGroups;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.DEST_CATALOG_NAME));
    McCreateOssExternalTableAction mcCreateOssExternalTableAction =
        new McCreateOssExternalTableAction(
            this.getId() + ".CreateExternalTable",
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            config.get(JobConfiguration.DATA_SOURCE_OSS_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_SOURCE_OSS_ACCESS_KEY_SECRET),
            config.get(JobConfiguration.DATA_SOURCE_OSS_ROLE_ARN),
            config.get(JobConfiguration.DATA_SOURCE_OSS_BUCKET),
            config.get(JobConfiguration.DATA_SOURCE_OSS_PATH),
            config.get(JobConfiguration.DATA_SOURCE_OSS_ENDPOINT),
            ossTableMetaModel,
            this,
            context);
    dag.addVertex(mcCreateOssExternalTableAction);

    if (!ossTableMetaModel.getPartitionColumns().isEmpty()) {
      List<TableMetaModel> sourceGroups = partitionGroups
          .stream()
          .map(TablePartitionGroup::getSource)
          .collect(Collectors.toList());
      int idx = 0;
      for (TableMetaModel partitionGroup : sourceGroups) {
        McAddPartitionsAction mcAddExternalPartitionsAction = new McAddPartitionsAction(
            this.getId() + ".AddExternalPartitions.part." + idx,
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            partitionGroup,
            this,
            context);
        dag.addVertex(mcAddExternalPartitionsAction);
        dag.addEdge(mcCreateOssExternalTableAction, mcAddExternalPartitionsAction);
        idx += 1;
      }
    }

    McCreateTableAction mcCreateTableAction = new McCreateTableAction(
        this.getId() + ".CreateTable",
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
        executionProject,
        config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
        mcTableMetaModel,
        this,
        context);
    dag.addVertex(mcCreateTableAction);
    if (!mcTableMetaModel.getPartitionColumns().isEmpty()) {
      List<TableMetaModel> destGroups = partitionGroups
          .stream()
          .map(TablePartitionGroup::getDest)
          .collect(Collectors.toList());
      int idx = 0;
      for (TableMetaModel managedPartitionGroup : destGroups) {
        McAddPartitionsAction mcAddPartitionsAction = new McAddPartitionsAction(
            this.getId() + ".AddPartitions.part." + idx,
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            managedPartitionGroup,
            this,
            context);
        dag.addVertex(mcAddPartitionsAction);
        dag.addEdge(mcCreateTableAction, mcAddPartitionsAction);
        idx += 1;
      }
    }
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
