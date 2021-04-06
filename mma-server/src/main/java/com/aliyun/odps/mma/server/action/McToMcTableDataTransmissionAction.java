package com.aliyun.odps.mma.server.action;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.server.OdpsSqlUtils;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class McToMcTableDataTransmissionAction extends McSqlAction {

  private TableMetaModel source;
  private TableMetaModel dest;

  public McToMcTableDataTransmissionAction(
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
    return OdpsSqlUtils.getInsertOverwriteTableStatement(source, dest);
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
  public String getName() {
    return "Table data transmission";
  }

  @Override
  public List<List<Object>> getResult() {
    return ((McSqlActionInfo) actionInfo).getResult();
  }
}
