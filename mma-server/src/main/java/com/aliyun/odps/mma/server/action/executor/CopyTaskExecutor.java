package com.aliyun.odps.mma.server.action.executor;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.aliyun.odps.Task;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mma.server.action.info.CopyTaskActionInfo;

public class CopyTaskExecutor extends AbstractActionExecutor {
  private static final Logger LOG = LogManager.getLogger("ExecutorLogger");

  private static class CopyTaskCallable implements Callable<List<List<Object>>> {
    private final Odps odps;
    private final Task copyTask;
    private final String actionId;
    private final CopyTaskActionInfo actionInfo;

    CopyTaskCallable(
            Odps odps,
            Task copyTask,
            String actionId,
            CopyTaskActionInfo actionInfo) {
      this.odps = odps;
      this.copyTask = copyTask;
      this.actionId = actionId;
      this.actionInfo = actionInfo;

    }

    @Override
    public List<List<Object>> call() throws Exception {
      LOG.info("ActionId: {}, Executing copyTask: {}, properties {}",
              this.actionId,
              this.copyTask.getName(),
              this.copyTask.getProperties());
      Instance instance = this.odps.instances().create(odps.getDefaultProject(), copyTask);

      this.actionInfo.setInstanceId(instance.getId());
      LOG.info("ActionId: {}, InstanceId: {}", this.actionId, instance.getId());

      try {
        this.actionInfo.setLogView(this.odps.logview().generateLogView(instance, 72L));
        LOG.info("ActionId: {}, LogView {}", this.actionId, this.actionInfo.getLogView());
      } catch (OdpsException e) {
        LOG.warn("ActionId: {}, failed to generate logview", this.actionId);
      }

      instance.waitForSuccess();

      LOG.info("Action execute result: {}", instance.getTaskResults());
      return Collections.emptyList();
    }
  }

  public Future<List<List<Object>>> execute(
          Odps odps,
          Task copyTask,
          String actionId,
          CopyTaskActionInfo actionInfo) {
    CopyTaskCallable callable = new CopyTaskCallable(odps, copyTask, actionId, actionInfo);

    return this.executor.submit(callable);
  }

}
