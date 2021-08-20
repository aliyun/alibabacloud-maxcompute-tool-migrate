package com.aliyun.odps.mma.server.task;

import java.util.List;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.McDropPartitionAction;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McAddPartitionsAction;
import com.aliyun.odps.mma.server.action.McCreateTableAction;
import com.aliyun.odps.mma.server.action.McDropTableAction;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class HiveToMcTableSetUpTask extends DagTask {
  private TableMetaModel mcTableMetaModel;
  private List<TableMetaModel> partitionGroups;
  private Job job;

  public HiveToMcTableSetUpTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel mcTableMetaModel,
      List<TableMetaModel> partitionGroups,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.mcTableMetaModel = mcTableMetaModel;
    this.partitionGroups = partitionGroups;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.DEST_CATALOG_NAME));

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

    if (mcTableMetaModel.getPartitionColumns().isEmpty()) {
      McDropTableAction mcDropTableAction = new McDropTableAction(
          this.getId() + ".DropTable",
          config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
          config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
          executionProject,
          config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
          mcTableMetaModel,
          this,
          context);
      dag.addVertex(mcDropTableAction);
      dag.addEdge(mcDropTableAction, mcCreateTableAction);
    } else {
      int idx = 0;
      for (TableMetaModel managedPartitionGroup : partitionGroups) {
        McDropPartitionAction mcDropPartitionAction = new McDropPartitionAction(
            this.getId() + ".DropPartitions.part." + idx,
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            managedPartitionGroup,
            this,
            context);
        dag.addVertex(mcDropPartitionAction);
        dag.addEdge(mcCreateTableAction, mcDropPartitionAction);
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
        dag.addEdge(mcDropPartitionAction, mcAddPartitionsAction);
        idx += 1;
      }
    }
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
