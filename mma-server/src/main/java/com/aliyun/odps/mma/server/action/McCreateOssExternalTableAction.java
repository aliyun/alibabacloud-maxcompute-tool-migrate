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

import com.aliyun.odps.mma.config.ExternalTableStorage;
import com.aliyun.odps.mma.server.resource.Resource;
import com.aliyun.odps.mma.util.McSqlUtils;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class McCreateOssExternalTableAction extends McSqlAction {

  private TableMetaModel mcExternalTableMetaModel;

  public McCreateOssExternalTableAction(
      String id,
      String mcAccessKeyId,
      String mcAccessKeySecret,
      String mcExecutionProject,
      String mcEndpoint,
      TableMetaModel mcExternalTableMetaModel,
      Task task,
      ActionExecutionContext context) {
    super(id, mcAccessKeyId, mcAccessKeySecret, mcExecutionProject, mcEndpoint, task, context);
    this.mcExternalTableMetaModel = mcExternalTableMetaModel;
    resourceMap.put(Resource.METADATA_WORKER, 1L);
  }

  @Override
  public String getSql() {
    return McSqlUtils.getCreateTableStatement(mcExternalTableMetaModel, ExternalTableStorage.OSS);
  }

  @Override
  public boolean hasResults() {
    return false;
  }

  @Override
  public Map<String, String> getSettings() {
    HashMap<String, String> settings = new HashMap<>();
    settings.put("odps.sql.hive.compatible", "true");
    return settings;
  }

  @Override
  public String getName() {
    return "Create MC external Table";
  }
}
