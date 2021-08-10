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

package com.aliyun.odps.mma.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.action.Action;
import com.aliyun.odps.mma.server.action.executor.ActionExecutorFactory;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;

public class JobScheduler {
  private static final Logger LOG = LogManager.getLogger(JobScheduler.class);

  private static final int DEFAULT_SCHEDULING_INTERVAL_MS = 10000;
  private static final int DEFAULT_TASK_CACHE_SIZE = 1000;

  volatile boolean keepRunning;
  boolean ephemeral = false;

  final Set<Job> runningJobs;
  final Set<Task> runningTasks;
  final List<Task> failedTasks;
  final CircularFifoBuffer succeededTasks;
  final CircularFifoBuffer canceledTasks;
  final List<Action> runningActions;

  public JobScheduler() {
    this.keepRunning = true;

    this.runningJobs = Collections.synchronizedSet(new HashSet<>());
    this.runningTasks = Collections.synchronizedSet(new HashSet<>());
    this.failedTasks = Collections.synchronizedList(new LinkedList<>());
    this.succeededTasks = new CircularFifoBuffer(DEFAULT_TASK_CACHE_SIZE);
    this.canceledTasks = new CircularFifoBuffer(DEFAULT_TASK_CACHE_SIZE);

    this.runningActions = Collections.synchronizedList(new LinkedList<>());
  }

  public JobScheduler(boolean ephemeral) {
    this();
    this.ephemeral = ephemeral;
  }

  public void schedule(Job job) {
    synchronized (runningJobs) {

      // TODO: a better way to print to the dw console
      if (ephemeral) {
        System.err.println("Schedule job, job id: " + job.getId());
      }

      runningJobs.add(job);
    }
  }

  public List<Task> getRunningTasks() {
    return new ArrayList<>(runningTasks);
  }

  public List<Task> getFailedTasks() {
    return new ArrayList<>(failedTasks);
  }

  public List<Task> getSucceededTasks() {
    return new ArrayList<>(succeededTasks);
  }

  public List<Task> getCanceledTasks() {
    return new ArrayList<>(canceledTasks);
  }

  public void shutdown() throws MmaException {
    LOG.info("Shutdown task runners.");

    keepRunning = false;
    synchronized (runningTasks) {
      for (Task t : runningTasks) {
        t.stop();
      }
    }
    ActionExecutorFactory.shutdown();
  }

  public void run() {
    while (keepRunning) {
      try {
        handleTerminatedJobs();
        handleTerminatedTasks();
        handleTerminatedActions();

        synchronized (runningJobs) {
          for (Job job : runningJobs) {
            List<Task> tasks = job.getExecutableTasks();

            // TODO: a better way to print to the dw console
            if (ephemeral) {
              for (Task task : tasks) {
                System.err.println(
                    "Schedule task, job id: " + job.getId() + ", task id: " + task.getId());
              }
            }

            runningTasks.addAll(tasks);
          }
        }

        synchronized (runningTasks) {
          for (Task task : runningTasks) {

            List<Action> executableActions = task.getExecutableActions();
            for (Action action : executableActions) {
              if (action.tryAllocateResource()) {
                action.execute();

                // TODO: a better way to print to the dw console
                if (ephemeral) {
                  System.err.println(
                      "Schedule action, task id" + task.getId() + ", action id: " + action.getId());
                }

                runningActions.add(action);
              }
            }
          }
        }
        Thread.sleep(DEFAULT_SCHEDULING_INTERVAL_MS);
      } catch (Throwable e) {
        LOG.error("Exception on scheduling thread", e);
      }
    }
  }

  void handleTerminatedJobs() {
    LOG.info("Enter handleTerminatedJobs");
    List<Job> terminatedJobs = new LinkedList<>();
    for (Job job : runningJobs) {
      // TODO: remove later
      LOG.info("Job id: {}, status: {}", job.getId(), job.getStatus());
      if (JobStatus.SUCCEEDED.equals(job.getStatus())
          || JobStatus.FAILED.equals(job.getStatus())
          || JobStatus.CANCELED.equals(job.getStatus())) {
        LOG.info("Job terminated, id: {}, status: {}", job.getId(), job.getStatus());

        // TODO: a better way to print to the dw console
        if (ephemeral) {
          String msg = String.format(
              "Job terminated, id: %s, status: %s",
              job.getId(),
              job.getStatus().name());
          System.err.println(msg);
        }

        if (JobStatus.FAILED.equals(job.getStatus()) && job.retry()) {
          // Retry the job
          continue;
        }
        terminatedJobs.add(job);
      }
    }

    runningJobs.removeAll(terminatedJobs);
    if (!terminatedJobs.isEmpty() && ephemeral) {
      keepRunning = false;
    }
  }

  void handleTerminatedTasks() {
    LOG.info("Enter handleTerminatedTasks");
    List<Task> terminatedTasks = new LinkedList<>();
    for (Task task : runningTasks) {
      if (TaskProgress.SUCCEEDED.equals(task.getProgress())
          || TaskProgress.FAILED.equals(task.getProgress())
          || TaskProgress.CANCELED.equals(task.getProgress())) {
        LOG.info("Task terminated, id: {}, status: {}", task.getId(), task.getProgress());

        // TODO: a better way to print to the dw console
        if (ephemeral) {
          String msg = String.format(
              "Task terminated, id: %s, status: %s",
              task.getId(),
              task.getProgress().name());
          System.err.println(msg);
        }

        switch (task.getProgress()) {
          case FAILED:
              failedTasks.add(task);
            break;
          case SUCCEEDED:
              succeededTasks.add(task);
            break;
          case CANCELED:
              canceledTasks.add(task);
            break;
          default:
        }
        terminatedTasks.add(task);
      }
    }

    runningTasks.removeAll(terminatedTasks);
  }

  void handleTerminatedActions() {
    LOG.info("Enter handleTerminatedActions");
    List<Action> terminatedActions = new LinkedList<>();
    for (Action action : runningActions) {
      if (action.executionFinished()) {
        try {
          action.afterExecution();
        } catch (Exception e) {
          LOG.error(
              "Action terminated with failure, id: {}, stack trace: {}",
              action.getId(),
              ExceptionUtils.getStackTrace(e));
        } finally {
          LOG.info(
              "Action terminated, id: {}, status: {}",
              action.getId(),
              action.getProgress());

          // TODO: a better way to print to the dw console
          if (ephemeral) {
            String msg = String.format(
                "Action terminated, id: %s, status: %s",
                action.getId(),
                action.getProgress().name());
            System.err.println(msg);
          }

          terminatedActions.add(action);
          action.releaseResource();
        }
      }
    }
    runningActions.removeAll(terminatedActions);
  }
}
