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

import com.aliyun.odps.mma.util.HiveSqlUtils;
import com.aliyun.odps.mma.server.action.info.HiveSqlActionInfo;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class HiveVerificationAction extends HiveSqlAction {

  private boolean isSourceVerification;
  private TableMetaModel tableMetaModel;

  public HiveVerificationAction(
      String id,
      String jdbcUrl,
      String username,
      String password,
      TableMetaModel tableMetaModel,
      boolean isSourceVerification,
      Map<String, String> userHiveSettings,
      Task task,
      ActionExecutionContext actionExecutionContext) {
    super(id, jdbcUrl, username, password, userHiveSettings, task, actionExecutionContext);
    this.isSourceVerification = isSourceVerification;
    this.tableMetaModel = tableMetaModel;
  }

  @Override
  public String getSql() {
    return HiveSqlUtils.getVerifySql(tableMetaModel);
  }

  @Override
  public String getName() {
    return "Hive data verification";
  }

  @Override
  public List<List<Object>> getResult() {
    return ((HiveSqlActionInfo) actionInfo).getResult();
  }

  @Override
  void handleResult(List<List<Object>> result) {
    result = ActionUtils.toVerificationResult(result);
    ((HiveSqlActionInfo) actionInfo).setResult(result);
    if (isSourceVerification) {
      actionExecutionContext.setSourceVerificationResult(result);
    } else {
      actionExecutionContext.setDestVerificationResult(result);
    }
  }
}
