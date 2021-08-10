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

package com.aliyun.odps.mma.server.action;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.action.info.AbstractActionInfo;
import com.aliyun.odps.mma.server.resource.Resource;
import com.aliyun.odps.mma.server.resource.MmaResourceManager;
import com.aliyun.odps.mma.server.task.Task;

public abstract class AbstractAction<T> implements Action {
  private static final Logger LOG = LogManager.getLogger(AbstractAction.class);

  private ActionProgress progress;
  private Long startTime;
  private Long endTime;

  Map<Resource, Long> resourceMap;
  Future<T> future;

  /**
   * Used by sub classes
   */
  String id;
  AbstractActionInfo actionInfo;
  Task task;
  ActionExecutionContext actionExecutionContext;

  public AbstractAction(
      String id,
      Task task,
      ActionExecutionContext actionExecutionContext) {
    this.id = Objects.requireNonNull(id);
    this.progress = ActionProgress.PENDING;
    this.task = Objects.requireNonNull(task);
    this.actionExecutionContext = Objects.requireNonNull(actionExecutionContext);
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
  public Long getStartTime() {
    return startTime;
  }

  @Override
  public Long getEndTime() {
    return endTime;
  }

  @Override
  public AbstractActionInfo getActionInfo() {
    return actionInfo;
  }

  @Override
  public boolean tryAllocateResource() {
    Map<Resource, Long> finalResourceMap =
        MmaResourceManager.getInstance().allocate(id, resourceMap);
    if (finalResourceMap != null) {
      resourceMap = finalResourceMap;
      return true;
    }

    return false;
  }

  @Override
  public void releaseResource() {
    MmaResourceManager.getInstance().release(id, resourceMap);
  }

  @Override
  public void execute() throws MmaException {
    setProgress(ActionProgress.RUNNING);
    try {
      executeInternal();
    } catch (Exception e) {
      LOG.error(
          "Action failed before execution, actionId: {}, reason: {}",
          id,
          ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  abstract void executeInternal() throws Exception;

  @Override
  public void afterExecution() {
    if (isTerminated()) {
      LOG.info("Action already terminated, action id: {}", id);
      return;
    }

    try {
      if (future != null) {
        T result = future.get();
        handleResult(result);
      }
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id,
                ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  abstract void handleResult(T result);

  @Override
  public boolean executionFinished() {
    if (ActionProgress.FAILED.equals(getProgress())
        || ActionProgress.SUCCEEDED.equals(getProgress())
        || ActionProgress.CANCELED.equals(getProgress())) {
      return true;
    }

    if (future == null) {
      throw new IllegalStateException("Action not executed, actionId: " + id);
    }

    return future.isDone();
  }

  private void setProgress(ActionProgress progress) {
    LOG.info("Set action status, id: {}, from: {}, to: {}", id, this.progress, progress);

    if (ActionProgress.PENDING.equals(this.progress) && ActionProgress.RUNNING.equals(progress)) {
      startTime = System.currentTimeMillis();
    }

    this.progress = Objects.requireNonNull(progress);

    if (isTerminated()) {
      endTime = System.currentTimeMillis();
    }

    task.setStatus(this);
  }

  private boolean isTerminated() {
    return ActionProgress.CANCELED.equals(progress)
        || ActionProgress.FAILED.equals(progress)
        || ActionProgress.SUCCEEDED.equals(progress);
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public void stop() {
    setProgress(ActionProgress.CANCELED);
    if (this.future != null) {
      this.future.cancel(true);
    }
  }
}
