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