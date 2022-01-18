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
import com.aliyun.odps.mma.meta.model.TableMetaModel;

public class McToOssTableCleanUpTask extends DagTask {

  private TableMetaModel ossTableMetaModel;
  private Job job;

  public McToOssTableCleanUpTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel ossTableMetaModel,
      Job job) {
    super(id, rootJobId, config);
    this.ossTableMetaModel = ossTableMetaModel;
    this.job = job;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.SOURCE_CATALOG_NAME));
    McDropTableAction action = new McDropTableAction(
        id + ".DropTable",
        config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_ID),
        config.get(JobConfiguration.DATA_SOURCE_MC_ACCESS_KEY_SECRET),
        executionProject,
        config.get(JobConfiguration.DATA_SOURCE_MC_ENDPOINT),
        ossTableMetaModel,
        this,
        context);
    dag.addVertex(action);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }

  @Override
  public String getJobId() {
    // TODO:
    return null;
  }
}
