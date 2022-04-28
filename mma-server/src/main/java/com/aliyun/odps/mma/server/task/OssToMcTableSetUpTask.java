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

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McAddPartitionsAction;
import com.aliyun.odps.mma.server.action.McCreateOssExternalTableAction;
import com.aliyun.odps.mma.server.action.McCreateTableAction;
import com.aliyun.odps.mma.server.job.AbstractTableJob.TablePartitionGroup;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class OssToMcTableSetUpTask extends DagTask {
  private TableMetaModel mcExternalTableMetaModel;
  private TableMetaModel mcTableMetaModel;
  private List<TablePartitionGroup> partitionGroups;
  private Job job;

  public OssToMcTableSetUpTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel mcExternalTableMetaModel,
      TableMetaModel mcTableMetaModel,
      List<TablePartitionGroup> partitionGroups,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.mcExternalTableMetaModel = mcExternalTableMetaModel;
    this.mcTableMetaModel = mcTableMetaModel;
    this.partitionGroups = partitionGroups;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.DEST_CATALOG_NAME));
    mcExternalTableMetaModel = new TableMetaModel.TableMetaModelBuilder(mcExternalTableMetaModel)
        .serDe(config.get(AbstractConfiguration.DATA_SOURCE_OSS_FILE_TYPE)).build();
    McCreateOssExternalTableAction mcCreateOssExternalTableAction =
        new McCreateOssExternalTableAction(
            this.getId() + ".CreateExternalTable",
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            mcExternalTableMetaModel,
            this,
            context);
    dag.addVertex(mcCreateOssExternalTableAction);

    if (!mcExternalTableMetaModel.getPartitionColumns().isEmpty()) {
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
