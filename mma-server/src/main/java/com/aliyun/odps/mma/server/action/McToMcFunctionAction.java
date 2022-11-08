package com.aliyun.odps.mma.server.action;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Function;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.action.info.DefaultActionInfo;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;

public class McToMcFunctionAction extends DefaultAction {
  private final Result result = new Result();

  private final JobConfiguration config;
  private final Odps srcOdps;
  private final Odps destOdps;

  public McToMcFunctionAction(String id, Task task, ActionExecutionContext context,
                              JobConfiguration config, Odps srcOdps, Odps destOdps){
    super(id, task, context);
    this.config = config;
    this.srcOdps = srcOdps;
    this.destOdps = destOdps;
  }

  @Override
  void handleResult(Object result) {
    ((DefaultActionInfo) actionInfo).setResult(result.toString());
  }

  @Override
  public String getName() {
    return "McToMC Function transmission";
  }

  @Override
  public Object getResult() {
    if (ActionProgress.FAILED.equals(getProgress())) {
      if (result.getAll().size() == 1) {
        result.setFailed(result.getAll());
        result.setReason(getReason());
      } else {
        List<String> var1 = new ArrayList<>(result.getAll());
        var1.removeAll(result.getSuccess());
        result.setFailed(var1);
        result.setReason(String.format(
                "Maybe the first function has sync failed which in the failed list, detail: %s.",
                getReason()));
      }
      ((DefaultActionInfo) actionInfo).setResult(GsonUtils.GSON.toJson(result, Result.class));
    }
    return ((DefaultActionInfo) actionInfo).getResult();
  }

  @Override
  public Object call() throws Exception {
    List<McFunctionInfo> sync = new ArrayList<>();
    if (config.containsKey(JobConfiguration.SOURCE_OBJECT_NAME)
            && !config.get(JobConfiguration.SOURCE_OBJECT_NAME).isEmpty()) {
      result.getAll().add(config.get(JobConfiguration.SOURCE_OBJECT_NAME));
      Function function = OdpsUtils.getFunction(
              srcOdps,
              config.get(JobConfiguration.SOURCE_CATALOG_NAME),
              config.get(JobConfiguration.SOURCE_OBJECT_NAME));
      sync.add(new McFunctionInfo(function));
    } else {
      for (Function function : srcOdps.functions()) {
        result.getAll().add(function.getName());
        sync.add(new McFunctionInfo(function));
      }
    }

    for (McFunctionInfo mcFunctionInfo : sync) {
      OdpsUtils.createFunction(destOdps, destOdps.getDefaultProject(), mcFunctionInfo, true);
      result.getSuccess().add(mcFunctionInfo.getFunctionName());
    }

    return GsonUtils.GSON.toJson(result, Result.class);
  }

  @Setter
  @Getter
  private static class Result {
    private List<String> all;
    private List<String> success;
    private List<String> failed;
    private String reason;

    Result () {
      this.all = new ArrayList<>();
      this.success = new ArrayList<>();
      this.failed = new ArrayList<>();
      this.reason = "";
    }

    @Override
    public String toString() {
      return "[all]: " + all.toString()
              + "\n[success]: " + success.toString()
              + "\n[failed]: " + failed.toString()
              + "\n[reason]: " + reason;
    }
  }
}
