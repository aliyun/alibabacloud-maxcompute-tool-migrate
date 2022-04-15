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

import com.aliyun.odps.mma.util.McSqlUtils;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class McInsertOverwriteAction extends McSqlAction {

  private TableMetaModel source;
  private TableMetaModel dest;

  public McInsertOverwriteAction(
      String id,
      String mcAccessKeyId,
      String mcAccessKeySecret,
      String mcProject,
      String mcEndpoint,
      TableMetaModel source,
      TableMetaModel dest,
      Task task,
      ActionExecutionContext context) {
    super(id, mcAccessKeyId, mcAccessKeySecret, mcProject, mcEndpoint, task, context);
    this.source = source;
    this.dest = dest;
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
    // TODO:
    return new HashMap<>();
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
