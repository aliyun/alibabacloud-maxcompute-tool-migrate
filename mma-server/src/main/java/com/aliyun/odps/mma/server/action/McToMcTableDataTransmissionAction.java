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

import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.util.McSqlUtils;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class McToMcTableDataTransmissionAction extends McSqlAction {

  private TableMetaModel source;
  private TableMetaModel dest;
  private Map<String, String> settings;

  public McToMcTableDataTransmissionAction(
      String id,
      String mcAccessKeyId,
      String mcAccessKeySecret,
      String mcProject,
      String mcEndpoint,
      TableMetaModel source,
      TableMetaModel dest,
      Task task,
      ActionExecutionContext context) throws MmaException {
    super(id, mcAccessKeyId, mcAccessKeySecret, mcProject, mcEndpoint, task, context);
    this.source = source;
    this.dest = dest;

    JobConfiguration config = context.getConfig();
    settings = ConfigurationUtils.getSQLSettings(
        config.get(AbstractConfiguration.JOB_EXECUTION_MC_SETTINGS));
  }

  @Override
  void handleResult(List<List<Object>> result) {
    ((McSqlActionInfo) actionInfo).setResult(result);
  }

  @Override
  public String getSql() {
    return McSqlUtils.getInsertOverwriteTableStatement(source, dest);
  }

  @Override
  public boolean hasResults() {
    return false;
  }

  @Override
  public Map<String, String> getSettings() {
    return settings;
  }

  @Override
  public String getName() {
    return "Table data transmission";
  }

  @Override
  public List<List<Object>> getResult() {
    return ((McSqlActionInfo) actionInfo).getResult();
  }
}
