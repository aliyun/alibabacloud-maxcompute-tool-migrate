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

import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.McSqlUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class McVerificationAction extends McSqlAction {

  private boolean isSourceVerification;
  private TableMetaModel tableMetaModel;

  public McVerificationAction(
      String id,
      String mcAccessKeyId,
      String mcAccessKeySecret,
      String mcProject,
      String mcEndpoint,
      TableMetaModel tableMetaModel,
      boolean isSourceVerification,
      Task task,
      ActionExecutionContext context) {
    super(id, mcAccessKeyId, mcAccessKeySecret, mcProject, mcEndpoint, task, context);
    this.tableMetaModel = tableMetaModel;
    this.isSourceVerification = isSourceVerification;
  }

  @Override
  public String getSql() {
    return McSqlUtils.getTableHashSql(tableMetaModel);
  }

  @Override
  public boolean hasResults() {
    return true;
  }

  @Override
  public Map<String, String> getSettings() {
    // TODO:
    return new HashMap<>();
  }

  @Override
  void handleResult(List<List<Object>> result) {
    result = ActionUtils.toVerificationResult(result);
    ((McSqlActionInfo) actionInfo).setResult(result);
    if (isSourceVerification) {
      actionExecutionContext.setSourceVerificationResult(result);
    } else {
      actionExecutionContext.setDestVerificationResult(result);
    }
    actionExecutionContext.addData("odps_table_hash", result);
  }

  @Override
  public String getName() {
    return "MC data Verification";
  }
}
