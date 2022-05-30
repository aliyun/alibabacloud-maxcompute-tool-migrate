/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.server.task;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.action.Action;
import com.aliyun.odps.mma.server.action.ActionProgress;

public abstract class DagTask implements Task {

  private static final Logger LOG = LogManager.getLogger(DagTask.class);

  private Long startTime;
  private Long endTime;

  String id;
  String rootJobId;
  TaskProgress status = TaskProgress.PENDING;
  DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
  JobConfiguration config;

  public DagTask(String id, String rootJobId, JobConfiguration config) {
    this.id = Objects.requireNonNull(id);
    this.rootJobId = Objects.requireNonNull(rootJobId);
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public DirectedAcyclicGraph<Action, DefaultEdge> getDag() {
    return dag;
  }

  @Override
  public Long getStartTime() {
    return startTime;
  }

  @Override
  public Long getEndTime() {
    return endTime;
  }

  private boolean isTerminated() {
    return TaskProgress.CANCELED.equals(status)
        || TaskProgress.SUCCEEDED.equals(status)
        || TaskProgress.FAILED.equals(status);
  }

  @Override
  public TaskProgress getProgress() {
    return status;
  }

  @Override
  public void setStatus(Action action) {
    if (isTerminated()) {
      return;
    }

    ActionProgress actionStatus = action.getProgress();

    TaskProgress oldStatus = status;
    status = status.next(dag, actionStatus);

    if (!oldStatus.equals(status)) {
      LOG.info("Set task status, id: {}, from: {}, to: {}", id, oldStatus, status);

      // Update start time
      if (TaskProgress.PENDING.equals(oldStatus) && TaskProgress.RUNNING.equals(status)) {
        startTime = System.currentTimeMillis();
      }

      if (isTerminated()) {
        endTime = System.currentTimeMillis();
      }

      updateMetadata();
    }
  }

  @Override
  public List<Action> getExecutableActions() {
    if (dag == null) {
      return Collections.emptyList();
    }

    List<Action> ret = new LinkedList<>();

    for (Action a : dag.vertexSet()) {
      boolean executable = ActionProgress.PENDING.equals(a.getProgress())
          && Graphs.predecessorListOf(dag, a)
                   .stream()
                   .allMatch(p -> ActionProgress.SUCCEEDED.equals(p.getProgress()));

      if (executable) {
        ret.add(a);
      }
    }

    if (ret.size() > 0) {
      LOG.info("Executable actions, task id: {}, ret: {}", id, ret);
    }
    return ret;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public void stop() throws MmaException {
    if (isTerminated()) {
      LOG.info("Stop terminated task, task id: {}", id);
      return;
    }
    this.status = TaskProgress.CANCELED;
    for (Action a : this.dag) {
      a.stop();
    }
  }

  @Override
  public String getRootJobId() {
    return rootJobId;
  }

  @Override
  public String getJobId() {
    // TODO: useless
    return null;
  }

  abstract void updateMetadata();


  public void save(String sql, String path, boolean append) {
    try {
      File file = new File(path);
      if (!file.exists()) {
        file.createNewFile();
      }
      FileWriter fw = new FileWriter(file.getAbsoluteFile(), append);
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(sql);
      bw.close();
    } catch (IOException e) {
      try {
        savesqlfail(config, e);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
      throw new RuntimeException(e);
    }
  }

  public void save(List<String> sql, String path, boolean append) {
    save(String.join(";\n", sql), path, append);
  }

  public String getPath(JobConfiguration config) {
    String prefix = config.get(AbstractConfiguration.SQL_SAVE_PATH);
    String db = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String table = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
    String[] parts = {prefix, db, table};
    String path = String.join(File.separator, parts);
    File dir = new File(path);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    return path;
  }

  public void savesqlfail(JobConfiguration config, Exception e) throws IOException {
    String fail = getPath(config) + File.separator + "fail.txt";
    File failFile = new File(fail);
    if (!failFile.exists()) {
      failFile.createNewFile();
    }
    FileWriter fw = new FileWriter(fail, true);
    fw.write(ExceptionUtils.getStackTrace(e));
    fw.write("\n");
    fw.close();

    String prefix = config.get(AbstractConfiguration.SQL_SAVE_PATH);
    String failedLog = prefix + File.separator + "failed.log";
    FileWriter fw2 = new FileWriter(failedLog, true);
    fw2.write(config.get(JobConfiguration.SOURCE_CATALOG_NAME + "." + JobConfiguration.SOURCE_OBJECT_NAME) + "\n");
  }

}
