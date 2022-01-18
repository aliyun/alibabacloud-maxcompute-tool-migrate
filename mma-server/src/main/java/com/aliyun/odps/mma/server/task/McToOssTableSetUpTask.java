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

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McAddPartitionsAction;
import com.aliyun.odps.mma.server.action.McCreateOssExternalTableAction;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.model.TableMetaModel;

public class McToOssTableSetUpTask extends DagTask {
  private TableMetaModel mcExternalMetaModel;
  private List<TableMetaModel> partitionGroupsDestMetaModels;
  private Job job;

  public McToOssTableSetUpTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel mcExternalMetaModel,
      List<TableMetaModel> partitionGroupsDestMetaModels,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.mcExternalMetaModel = mcExternalMetaModel;
    this.partitionGroupsDestMetaModels = partitionGroupsDestMetaModels;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.SOURCE_CATALOG_NAME));
    McCreateOssExternalTableAction mcCreateOssExternalTableAction =
        new McCreateOssExternalTableAction(
            this.getId() + ".CreateExternalTable",
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_SOURCE_MC_ENDPOINT),
            mcExternalMetaModel,
            this,
            context);
    dag.addVertex(mcCreateOssExternalTableAction);

    if (!mcExternalMetaModel.getPartitionColumns().isEmpty()) {
      int idx = 0;
      for (TableMetaModel groupDestMetaModel: partitionGroupsDestMetaModels) {
        McAddPartitionsAction mcAddPartitionsAction = new McAddPartitionsAction(
            this.getId() + ".AddExternalPartitions.part." + idx,
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_SECRET),
            config.get(JobConfiguration.SOURCE_CATALOG_NAME),
            config.get(JobConfiguration.DATA_SOURCE_MC_ENDPOINT),
            groupDestMetaModel,
            this,
            context);
        dag.addVertex(mcAddPartitionsAction);
        dag.addEdge(mcCreateOssExternalTableAction, mcAddPartitionsAction);
        idx += 1;
      }
    }
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
