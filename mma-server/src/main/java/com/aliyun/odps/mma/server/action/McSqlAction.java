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

import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.server.action.executor.ActionExecutorFactory;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.mma.server.task.Task;

public abstract class McSqlAction extends AbstractAction<List<List<Object>>> {

  private String accessKeyId;
  private String accessKeySecret;
  private String executionProject;
  private String endpoint;

  public McSqlAction(
      String id,
      OdpsConfig odpsConfig,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    actionInfo = new McSqlActionInfo();
    this.accessKeyId = odpsConfig.getAccessId();
    this.accessKeySecret = odpsConfig.getAccessKey();
    this.executionProject = odpsConfig.getProjectName();
    this.endpoint = odpsConfig.getEndpoint();
  }

  @Override
  void executeInternal() {
    future = ActionExecutorFactory.getMcSqlExecutor().execute(
        endpoint,
        executionProject,
        accessKeyId,
        accessKeySecret,
        getSql(),
        hasResults(),
        getSettings(),
        id,
        (McSqlActionInfo) actionInfo);
  }

  @Override
  public Object getResult() {
    return ((McSqlActionInfo) actionInfo).getResult();
  }

  @Override
  void handleResult(List<List<Object>> result) {
    ((McSqlActionInfo) actionInfo).setResult(result);
  }

  public abstract String getSql();

  public abstract boolean hasResults();

  public abstract Map<String, String> getSettings();
}
