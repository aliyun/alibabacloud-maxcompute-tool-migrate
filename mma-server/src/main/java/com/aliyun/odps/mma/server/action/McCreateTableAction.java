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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.server.resource.Resource;
import com.aliyun.odps.mma.util.McSqlUtils;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class McCreateTableAction extends McSqlAction {

  private TableMetaModel tableMetaModel;

  public McCreateTableAction(
      String id,
      String accessKeyId,
      String accessKeySecret,
      String executionProject,
      String endpoint,
      TableMetaModel tableMetaModel,
      Task task,
      ActionExecutionContext context) {
    super(id, accessKeyId, accessKeySecret, executionProject, endpoint, task, context);
    this.tableMetaModel = tableMetaModel;
    resourceMap.put(Resource.METADATA_WORKER, 1L);
  }

  @Override
  public String getSql() {
    return McSqlUtils.getCreateTableStatement(tableMetaModel);
  }

  @Override
  public boolean hasResults() {
    return false;
  }

  @Override
  public Map<String, String> getSettings() {
    /**
     * 如果create table的schema里包含TINYINT, SMALLINT, INT, CHAR, VARCHAR等2.0数据类型
     * 而dest project暂未开启2.0模式，便会导致create table失败
     * solution: set odps.sql.type.system.odps2=true
    */
    Map<String, String> hints = new HashMap<>();
    List<String> columnTypes = Arrays.asList("TINYINT", "SMALLINT", "INT", "CHAR", "VARCHAR");
    for (MetaSource.ColumnMetaModel columnMetaModel : tableMetaModel.getColumns()) {
      if (columnTypes.contains(columnMetaModel.getType().toUpperCase())) {
        hints.put("odps.sql.type.system.odps2", "true");
        break;
      }
    }
    return hints;
  }

  @Override
  public String getName() {
    return "Create MC Table";
  }
}
