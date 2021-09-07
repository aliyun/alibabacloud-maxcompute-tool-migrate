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
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class McToOssTableSetUpTask extends DagTask {
  private TableMetaModel ossTableMetaModel;
  private List<TableMetaModel> partitionGroups;
  private Job job;

  public McToOssTableSetUpTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel ossTableMetaModel,
      List<TableMetaModel> partitionGroups,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.ossTableMetaModel = ossTableMetaModel;
    this.partitionGroups = partitionGroups;
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
            config.get(JobConfiguration.DATA_DEST_OSS_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_OSS_ACCESS_KEY_SECRET),
            config.get(JobConfiguration.DATA_DEST_OSS_ROLE_ARN),
            config.get(JobConfiguration.DATA_DEST_OSS_BUCKET),
            null,
            config.get(JobConfiguration.DATA_DEST_OSS_ENDPOINT),
            ossTableMetaModel,
            this,
            context);
    dag.addVertex(mcCreateOssExternalTableAction);

    if (!ossTableMetaModel.getPartitionColumns().isEmpty()) {
      int idx = 0;
      for (TableMetaModel partitionGroup : partitionGroups) {
        McAddPartitionsAction mcAddPartitionsAction = new McAddPartitionsAction(
            this.getId() + ".AddExternalPartitions.part." + idx,
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_SECRET),
            config.get(JobConfiguration.SOURCE_CATALOG_NAME),
            config.get(JobConfiguration.DATA_SOURCE_MC_ENDPOINT),
            partitionGroup,
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
