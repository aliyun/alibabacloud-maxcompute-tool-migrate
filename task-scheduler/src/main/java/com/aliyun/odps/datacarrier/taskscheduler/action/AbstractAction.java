/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.AbstractActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;
import com.aliyun.odps.datacarrier.taskscheduler.resource.ResourceAllocator;
import com.aliyun.odps.datacarrier.taskscheduler.task.AbstractTask.ActionProgressListener;
import com.aliyun.odps.datacarrier.taskscheduler.task.ActionExecutionContext;

public abstract class AbstractAction implements Action {

  static final Logger LOG = LogManager.getLogger(AbstractAction.class);

  private ActionProgress progress;
  private ActionProgressListener actionProgressListener;

  protected Map<Resource, Integer> resourceMap;
  protected Future<Object> future;

  /**
   * Used by sub classes
   */
  String id;
  AbstractActionInfo actionInfo;
  ActionExecutionContext actionExecutionContext;

  public AbstractAction(String id) {
    this.id = Objects.requireNonNull(id);
    this.progress = ActionProgress.PENDING;
    this.resourceMap = new HashMap<>();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public ActionProgress getProgress() {
    return progress;
  }

  @Override
  public AbstractActionInfo getActionInfo() {
    return actionInfo;
  }

  @Override
  public boolean tryAllocateResource() {
    Map<Resource, Integer> finalResourceMap =
        ResourceAllocator.getInstance().allocate(id, resourceMap);
    if (finalResourceMap != null) {
      resourceMap = finalResourceMap;
      return true;
    }

    return false;
  }

  @Override
  public void releaseResource() {
    ResourceAllocator.getInstance().release(id, resourceMap);
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      future.get();
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id,
                ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  @Override
  public boolean executionFinished() {
    if (ActionProgress.FAILED.equals(getProgress())
        || ActionProgress.SUCCEEDED.equals(getProgress())) {
      return true;
    }

    if (future == null) {
      throw new IllegalStateException("Action not executed, actionId: " + id);
    }

    return future.isDone();
  }

  void setProgress(ActionProgress progress) throws MmaException {
    LOG.info("Update action progress, id: {}, cur progress: {}, new progress: {}",
             id,
             this.progress,
             progress);
    this.progress = Objects.requireNonNull(progress);

    actionProgressListener.onActionProgressChanged(progress);
  }

  public void setActionProgressListener(ActionProgressListener actionProgressListener) {
    this.actionProgressListener = Objects.requireNonNull(actionProgressListener);
  }

  public void setActionExecutionContext(ActionExecutionContext actionExecutionContext) {
    this.actionExecutionContext = Objects.requireNonNull(actionExecutionContext);
  }

  @Override
  public String toString() {
    return id;
  }
}
