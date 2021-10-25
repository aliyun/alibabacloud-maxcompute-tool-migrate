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

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McToOssFunctionAction;
import com.aliyun.odps.mma.server.action.McToOssResourceAction;
import com.aliyun.odps.mma.server.job.Job;

public class McToOssFunctionTask extends DagTask {

  private final OssConfig ossConfig;
  private final Job job;

  public McToOssFunctionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      OssConfig ossConfig,
      Job job) {
    super(id, rootJobId, config);
    this.job = job;
    this.ossConfig = ossConfig;
    init();
  }

  private void init() {
      ActionExecutionContext context = new ActionExecutionContext(config);

      Odps odps = OdpsUtils.getOdps(
          config.get(AbstractConfiguration.METADATA_SOURCE_MC_ACCESS_KEY_ID),
          config.get(AbstractConfiguration.METADATA_SOURCE_MC_ACCESS_KEY_SECRET),
          config.get(AbstractConfiguration.METADATA_SOURCE_MC_ENDPOINT),
          config.get(JobConfiguration.SOURCE_CATALOG_NAME)
      );

      String[] fileNames = OssUtils.getOssPaths(
          config.get(AbstractConfiguration.METADATA_DEST_OSS_PATH),
          rootJobId,
          config.get(JobConfiguration.OBJECT_TYPE),
          config.get(JobConfiguration.DEST_CATALOG_NAME),
          config.get(JobConfiguration.DEST_OBJECT_NAME));
      String metafile = fileNames[0];
      String datafile = fileNames[1] + config.get(JobConfiguration.DEST_OBJECT_NAME);

      McToOssFunctionAction action = new McToOssFunctionAction(
          id + ".Transmission",
          odps,
          config,
          ossConfig,
          metafile,
          datafile,
          this,
          context
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
