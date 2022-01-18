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
import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.action.executor.ActionExecutorFactory;
import com.aliyun.odps.mma.server.action.info.HiveSqlActionInfo;
import com.aliyun.odps.mma.server.task.Task;

public abstract class HiveSqlAction extends AbstractAction<List<List<Object>>> {

  private String jdbcUrl;
  private String username;
  private String password;
  Map<String, String> userHiveSettings;

  public HiveSqlAction(
      String id,
      String jdbcUrl,
      String username,
      String password,
      Map<String, String> userHiveSettings,
      Task task,
      ActionExecutionContext actionExecutionContext) {
    super(id, task, actionExecutionContext);
    actionInfo = new HiveSqlActionInfo();
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    if (null == userHiveSettings) {
      this.userHiveSettings = new HashMap<>();
    } else {
      this.userHiveSettings = userHiveSettings;
    }
  }

  @Override
  void executeInternal() throws Exception {
    future = ActionExecutorFactory.getHiveSqlExecutor().execute(
      jdbcUrl,
      username,
      password,
      getSql(),
      getSettings(),
      id,
      (HiveSqlActionInfo) actionInfo);
  }

  abstract String getSql() throws Exception;

  Map<String, String> getSettings() throws MmaException {
    return userHiveSettings;
  }
}
