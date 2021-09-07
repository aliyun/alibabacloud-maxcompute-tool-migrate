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

package com.aliyun.odps.mma.server.action;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.server.resource.Resource;
import com.aliyun.odps.mma.util.McSqlUtils;
import com.aliyun.odps.mma.server.OssExternalTableConfig;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class McCreateOssExternalTableAction extends McSqlAction {

  private String ossAccessKeyId;
  private String ossAccessKeySecret;
  private String ossRoleArn;
  private String ossBucket;
  private String ossPath;
  private String ossEndpoint;
  private TableMetaModel ossTableMetaModel;

  public McCreateOssExternalTableAction(
      String id,
      String mcAccessKeyId,
      String mcAccessKeySecret,
      String mcExecutionProject,
      String mcEndpoint,
      String ossAccessKeyId,
      String ossAccessKeySecret,
      String ossRoleArn,
      String ossBucket,
      String ossPath,
      String ossEndpoint,
      TableMetaModel ossTableMetaModel,
      Task task,
      ActionExecutionContext context) {
    super(id, mcAccessKeyId, mcAccessKeySecret, mcExecutionProject, mcEndpoint, task, context);
    this.ossAccessKeyId = ossAccessKeyId;
    this.ossAccessKeySecret = ossAccessKeySecret;
    this.ossRoleArn = ossRoleArn;
    this.ossBucket = ossBucket;
    this.ossPath = ossPath;
    this.ossEndpoint = ossEndpoint;
    this.ossTableMetaModel = ossTableMetaModel;
    resourceMap.put(Resource.METADATA_WORKER, 1L);
  }

  @Override
  public String getSql() {
    String tableDataLocation = OssUtils.getMcToOssDataPath(
        ossPath,
        task.getRootJobId(),
        ObjectType.TABLE.name(),
        ossTableMetaModel.getDatabase(),
        ossTableMetaModel.getLocation());
    OssConfig ossConfig = new OssConfig(
        ossEndpoint,
        ossEndpoint,
        ossBucket,
        ossRoleArn,
        ossAccessKeyId,
        ossAccessKeySecret);
    OssExternalTableConfig ossExternalTableConfig = new OssExternalTableConfig(
        ossConfig.getOssEndpoint(),
        ossConfig.getOssBucket(),
        ossConfig.getOssRoleArn(),
        McSqlUtils.getOssTablePath(ossConfig, tableDataLocation));

    return McSqlUtils.getCreateTableStatement(ossTableMetaModel, ossExternalTableConfig);
  }

  @Override
  public boolean hasResults() {
    return false;
  }

  @Override
  public Map<String, String> getSettings() {
    // TODO:
    return new HashMap<>();
  }

  @Override
  public String getName() {
    return "Create MC external Table";
  }
}
