package com.aliyun.odps.mma.server.action;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.mma.server.resource.Resource;
import com.aliyun.odps.mma.util.McSqlUtils;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.McSqlAction;
import com.aliyun.odps.mma.server.task.Task;

public class McDropPartitionAction extends McSqlAction {

  private TableMetaModel tableMetaModel;

  public McDropPartitionAction(
      String id,
      String mcAccessKeyId,
      String mcAccessKeySecret,
      String mcExecutionProject,
      String mcEndpoint,
      TableMetaModel mcTableMetaModel,
      Task task,
      ActionExecutionContext context) {
    super(id, mcAccessKeyId, mcAccessKeySecret, mcExecutionProject, mcEndpoint, task, context);
    this.tableMetaModel = mcTableMetaModel;
    resourceMap.put(Resource.METADATA_WORKER, 1L);
  }

  @Override
  public String getSql() {
    return McSqlUtils.getDropPartitionStatement(tableMetaModel);
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
    return "Drop MC Partitions";
  }
}