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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.generated.Job.JobBuilder;
import com.aliyun.odps.mma.server.task.Task;

public class PartitionJob extends AbstractJob {

  public PartitionJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public synchronized void setStatus(Task task) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Task> getExecutableTasks() {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean updateObjectMetadata() throws Exception {
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String partitionIdentifier = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
    String tableName = ConfigurationUtils.getTableNameFromPartitionIdentifier(partitionIdentifier);
    List<String> partitionValues =
        ConfigurationUtils.getPartitionValuesFromPartitionIdentifier(partitionIdentifier);
    PartitionMetaModel partitionMetaModel = metaSource.getPartitionMeta(
        catalogName,
        tableName,
        partitionValues);

    Long oldObjectLastModifiedTime =
        config.containsKey(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME) ?
            Long.valueOf(config.get(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME)) : null;
    Long newObjectLastModifiedTime = partitionMetaModel.getLastModificationTime();
    boolean objectChanged = oldObjectLastModifiedTime != null
        && newObjectLastModifiedTime != null
        && oldObjectLastModifiedTime < newObjectLastModifiedTime;
    if (objectChanged) {
      JobBuilder jobBuilder = new JobBuilder(record);
      Map<String, String> jobConfBuilder = new HashMap<>(config);
      jobConfBuilder.put(
          JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME,
          Long.toString(newObjectLastModifiedTime));
      JobConfiguration newJobConf = new JobConfiguration(jobConfBuilder);
      jobBuilder.jobConfig(newJobConf.toString());
      config = newJobConf;
      update(jobBuilder);
      return true;
    }
    return false;
  }
}
