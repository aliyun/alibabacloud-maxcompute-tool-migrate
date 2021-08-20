package com.aliyun.odps.mma.server.action;

import java.util.HashMap;
import java.util.Map;

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
    // TODO:
    return new HashMap<>();
  }

  @Override
  public String getName() {
    return "Create MC Table";
  }
}
