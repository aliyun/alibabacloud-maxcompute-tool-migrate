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

package com.aliyun.odps.mma.server.job;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.OssToMcFunctionTask;
import com.aliyun.odps.mma.server.task.Task;

public class OssToMcFunctionJob extends AbstractSingleTaskJob {

  public OssToMcFunctionJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  Task generateTask() throws Exception {
    String taskIdPrefix = generateTaskIdPrefix();

    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    FunctionMetaModel functionMetaModel = metaSource.getFunctionMeta(
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME));

    return new OssToMcFunctionTask(
        taskIdPrefix + ".functionTransmission",
        getRootJobId(),
        config,
        functionMetaModel,
         this);
  }

}
