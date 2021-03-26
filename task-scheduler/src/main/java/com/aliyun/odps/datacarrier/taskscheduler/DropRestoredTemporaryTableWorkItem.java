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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDropTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils;
import com.aliyun.odps.datacarrier.taskscheduler.task.ObjectRestoreTask;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProvider;

public class DropRestoredTemporaryTableWorkItem extends BackgroundWorkItem {
  private static final Logger LOG = LogManager.getLogger(DropRestoredTemporaryTableWorkItem.class);
  private String jobId;
  private String taskId;
  private String db;
  private String tbl;
  private MmaConfig.OdpsConfig odpsConfig;
  private MmaConfig.OssConfig ossConfig;
  private MetaSource.TableMetaModel tableMetaModel;
  private MmaMetaManager mmaMetaManager;
  private TaskProvider provider;

  private boolean finished = false;

  public DropRestoredTemporaryTableWorkItem(
      String jobId,
      String taskId,
      String db,
      String tbl,
      MmaConfig.OdpsConfig odpsConfig,
      MmaConfig.OssConfig ossConfig,
      MetaSource.TableMetaModel tableMetaModel,
      MmaMetaManager mmaMetaManager,
      TaskProvider provider) {
    this.jobId = Objects.requireNonNull(jobId);
    this.taskId = Objects.requireNonNull(taskId);
    this.db = Objects.requireNonNull(db);
    this.tbl = Objects.requireNonNull(tbl);
    this.odpsConfig = Objects.requireNonNull(odpsConfig);
    this.ossConfig = Objects.requireNonNull(ossConfig);
    this.tableMetaModel = tableMetaModel;
    this.mmaMetaManager = mmaMetaManager;
    this.provider = provider;
  }

  @Override
  public void execute() {
    try {
      MmaMetaManagerDbImplUtils.JobInfo jobInfo = mmaMetaManager.getMigrationJob(
          jobId,
          MmaConfig.JobType.MIGRATION.name(),
          MmaConfig.ObjectType.TABLE.name(),
          db,
          tbl);
      if (jobInfo == null) {
        LOG.info("db {}, tbl {} not found in meta db", db, tbl);
        finished = true;
        return;
      }
      MmaMetaManager.JobStatus status = jobInfo.getStatus();
      if (MmaMetaManager.JobStatus.SUCCEEDED.equals(status)) {
        LOG.info("Migration {}.{} in restore job {} succeed", db, tbl, jobId);
        OdpsDropTableAction dropTableAction =
            new OdpsDropTableAction(taskId, db, tbl, false);
        DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(
            DefaultEdge.class);
        dag.addVertex(dropTableAction);
        ObjectRestoreTask task = new ObjectRestoreTask(
            taskId, jobId, jobInfo.getObjectType().name(), tableMetaModel, dag, mmaMetaManager);
        task.setOdpsConfig(odpsConfig);
        task.setOssConfig(ossConfig);
        provider.addPendingTask(task);
        mmaMetaManager.removeMigrationJob(
            jobId,
            MmaConfig.JobType.MIGRATION.name(),
            MmaConfig.ObjectType.TABLE.name(),
            db, tbl);
        finished = true;
      } else if (MmaMetaManager.JobStatus.FAILED.equals(status)) {
        LOG.info("Migration {}.{} in restore job {} failed", db, tbl, jobId);
        finished = true;
      }
    } catch (MmaException e) {
      LOG.error("Exception when get job status for db {}, tbl {} ", db, tbl, e);
      finished = true;
    }
  }

  @Override
  public boolean finished() {
    return finished;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DropRestoredTemporaryTableWorkItem: [");
    sb.append("jobId=").append(jobId).append(", ")
      .append("taskId=").append(taskId).append(", ")
      .append("db=").append(db).append(", ")
      .append("tbl=").append(tbl).append(", ")
      .append("]");
    return sb.toString();
  }
}
