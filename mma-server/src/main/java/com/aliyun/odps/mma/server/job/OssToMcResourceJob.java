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

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.OssToMcResourceTask;
import com.aliyun.odps.mma.server.task.Task;

public class OssToMcResourceJob extends AbstractSingleTaskJob {

  public OssToMcResourceJob(
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
    OssConfig ossConfig = new OssConfig(
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_INTERNAL),
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_EXTERNAL),
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_BUCKET),
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ROLE_ARN),
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_ID),
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_SECRET),
        config.get(AbstractConfiguration.METADATA_SOURCE_OSS_PATH),
        getRootJobId());

    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    ResourceMetaModel resourceMetaModel = metaSource.getResourceMeta(
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME));

    return new OssToMcResourceTask(
        taskIdPrefix + ".resourceTransmission",
        getRootJobId(),
        config,
        resourceMetaModel,
        ossConfig,
         this);
  }

}
