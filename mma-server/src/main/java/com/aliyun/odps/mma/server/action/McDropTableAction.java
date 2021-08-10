package com.aliyun.odps.mma.server.action;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.mma.server.OdpsSqlUtils;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class McDropTableAction extends McSqlAction {

  TableMetaModel tableMetaModel;

  public McDropTableAction(
      String id,
      String mcAccessKeyId,
      String mcAccessKeySecret,
      String mcExecutionProject,
      String mcEndpoint,
      TableMetaModel tableMetaModel,
      Task task,
      ActionExecutionContext context) {
    super(id, mcAccessKeyId, mcAccessKeySecret, mcExecutionProject, mcEndpoint, task, context);
    this.tableMetaModel = tableMetaModel;
  }

  @Override
  public String getSql() {
    return OdpsSqlUtils.getDropTableStatement(
        tableMetaModel.getDatabase(),
        tableMetaModel.getTable());
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
    return "Drop table";
  }
}
