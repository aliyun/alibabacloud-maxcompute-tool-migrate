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
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.meta.OssMetaSource;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McToOssFunctionAction;
import com.aliyun.odps.mma.server.job.Job;

public class McToOssFunctionTask extends DagTask {

  private final OssConfig ossConfig;
  private final Job job;
  private final FunctionMetaModel functionMetaModel;

  public McToOssFunctionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      FunctionMetaModel functionMetaModel,
      OssConfig ossConfig,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.ossConfig = ossConfig;
    this.functionMetaModel = functionMetaModel;
    init();
  }

  private void init() {
      ActionExecutionContext context = new ActionExecutionContext(config);

      String[] fileNames = OssMetaSource.getMetaAndDataPath(
          ossConfig.getOssPrefix(),
          config.get(JobConfiguration.DEST_CATALOG_NAME),
          ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE)),
          config.get(JobConfiguration.DEST_OBJECT_NAME));
      String metafile = fileNames[0];

      McToOssFunctionAction action = new McToOssFunctionAction(
          id + ".Transmission",
          this,
          context,
          functionMetaModel,
          ossConfig,
          metafile
      );
      dag.addVertex(action);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }

  @Override
  public String getJobId() {
    // TODO: remove
    return null;
  }
}
