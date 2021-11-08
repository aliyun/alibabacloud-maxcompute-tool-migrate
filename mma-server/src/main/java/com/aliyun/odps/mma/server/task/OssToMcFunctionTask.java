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
 *
 */

package com.aliyun.odps.mma.server.task;

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.OssToMcFunctionAction;
import com.aliyun.odps.mma.server.job.Job;

public class OssToMcFunctionTask extends DagTask {

  private final Job job;
  private final FunctionMetaModel functionMetaModel;

  public OssToMcFunctionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      FunctionMetaModel functionMetaModel,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.functionMetaModel = functionMetaModel;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);

    Odps odps = OdpsUtils.getOdps(
        config.get(AbstractConfiguration.METADATA_DEST_MC_ACCESS_KEY_ID),
        config.get(AbstractConfiguration.METADATA_DEST_MC_ACCESS_KEY_SECRET),
        config.get(AbstractConfiguration.METADATA_DEST_MC_ENDPOINT),
        config.get(JobConfiguration.DEST_CATALOG_NAME)
    );

    OssToMcFunctionAction action = new OssToMcFunctionAction(
        id + ".Transmission",
        functionMetaModel,
        odps,
        true,
        this,
        context);
    dag.addVertex(action);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
