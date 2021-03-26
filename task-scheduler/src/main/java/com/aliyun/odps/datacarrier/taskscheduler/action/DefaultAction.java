package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.executor.ActionExecutorFactory;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.DefaultActionInfo;
import java.util.concurrent.Callable;


public abstract class DefaultAction extends AbstractAction implements Callable<Object>
{
  public DefaultAction(String id) {
    super(id);
    this.actionInfo = new DefaultActionInfo();
  }

  @Override
  public void execute() throws MmaException {
    setProgress(ActionProgress.RUNNING);
    this.future = ActionExecutorFactory.getDefaultExecutor().execute(this);
  }
}
