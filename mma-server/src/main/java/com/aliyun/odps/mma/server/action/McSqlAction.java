package com.aliyun.odps.mma.server.action;

import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.server.action.executor.ActionExecutorFactory;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.mma.server.task.Task;

public abstract class McSqlAction extends AbstractAction<List<List<Object>>> {

  private String accessKeyId;
  private String accessKeySecret;
  private String executionProject;
  private String endpoint;

  public McSqlAction(
      String id,
      String accessKeyId,
      String accessKeySecret,
      String executionProject,
      String endpoint,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    actionInfo = new McSqlActionInfo();
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
    this.executionProject = executionProject;
    this.endpoint = endpoint;
  }

  @Override
  void executeInternal() {
    future = ActionExecutorFactory.getMcSqlExecutor().execute(
        endpoint,
        executionProject,
        accessKeyId,
        accessKeySecret,
        getSql(),
        hasResults(),
        getSettings(),
        id,
        (McSqlActionInfo) actionInfo);
  }

  @Override
  public Object getResult() {
    return ((McSqlActionInfo) actionInfo).getResult();
  }

  @Override
  void handleResult(List<List<Object>> result) {
    ((McSqlActionInfo) actionInfo).setResult(result);
  }

  public abstract String getSql();

  public abstract boolean hasResults();

  public abstract Map<String, String> getSettings();
}
