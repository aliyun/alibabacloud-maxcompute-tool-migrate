/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.server.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.McDropPartitionAction;
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
      save(mcDropTableAction.getSql(), getPath(config) + File.separator + "data.sql", false);
      save(mcCreateTableAction.getSql(), getPath(config) + File.separator + "data.sql", true);
    } else {
      save(mcCreateTableAction.getSql(), getPath(config) + File.separator + "schema.sql", false);
      int idx = 0;
      // save("", getPath(config) + File.separator + "data." + idx + ".sql", false);
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
        save(mcDropPartitionAction.getSql(), getPath(config) + File.separator + "data." + idx + ".sql", false);
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
        save(mcAddPartitionsAction.getSql(), getPath(config) + File.separator + "data." + idx + ".sql", true);
        idx += 1;
      }
    }

  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
