package com.aliyun.odps.mma.server.action;

import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mma.server.action.executor.ActionExecutorFactory;
import com.aliyun.odps.mma.server.action.info.HiveSqlActionInfo;
import com.aliyun.odps.mma.server.task.Task;

public abstract class HiveSqlAction extends AbstractAction<List<List<Object>>> {

  private String jdbcUrl;
  private String username;
  private String password;

  public HiveSqlAction(
      String id,
      String jdbcUrl,
      String username,
      String password,
      Task task,
      ActionExecutionContext actionExecutionContext) {
    super(id, task, actionExecutionContext);
    actionInfo = new HiveSqlActionInfo();
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
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

  abstract Map<String, String> getSettings();
}
