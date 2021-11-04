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
import com.aliyun.odps.mma.config.OssConfig;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McToOssTableMetadataTransmissionAction;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class McToOssTableMetadataTransmissionTask extends DagTask {

  private TableMetaModel tableMetaModel;
  private OssConfig ossConfig;
  private Job job;

  public McToOssTableMetadataTransmissionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      OssConfig ossConfig,
      TableMetaModel tableMetaModel,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.tableMetaModel = tableMetaModel;
    this.ossConfig = ossConfig;
    init();
  }

  private void init() {
    ActionExecutionContext context = new ActionExecutionContext(config);
    McToOssTableMetadataTransmissionAction action = new McToOssTableMetadataTransmissionAction(
        id + ".MetadataTransmission",
        tableMetaModel,
        ossConfig,
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
    // TODO: remove
    return null;
  }
}
