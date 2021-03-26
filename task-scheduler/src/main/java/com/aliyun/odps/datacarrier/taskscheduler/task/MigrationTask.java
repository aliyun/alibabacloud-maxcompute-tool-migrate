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

package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.Objects;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.ActionProgress;
import com.aliyun.odps.datacarrier.taskscheduler.action.VerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.VerificationActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventManager;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaTaskFailedEvent;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaTaskSucceedEvent;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;

public class MigrationTask extends AbstractTask {

  private MetaSource.TableMetaModel tableMetaModel;
  // TODO: String?
  private String jobType;

  public MigrationTask(
      String id,
      String jobId,
      String jobType,
      MetaSource.TableMetaModel tableMetaModel,
      DirectedAcyclicGraph<Action, DefaultEdge> dag,
      MmaMetaManager mmaMetaManager) {
    super(id, jobId, dag, mmaMetaManager);
    this.jobType = jobType;
    this.actionExecutionContext.setTableMetaModel(Objects.requireNonNull(tableMetaModel));
  }

@Override
  void updateMetadata() throws MmaException {
    this.tableMetaModel = actionExecutionContext.getTableMetaModel();
    if (!tableMetaModel.partitionColumns.isEmpty()) {
      if (TaskProgress.SUCCEEDED.equals(progress)) {
        mmaMetaManager.updateStatus(
            jobId,
            jobType,
            MmaConfig.ObjectType.TABLE.name(),
            tableMetaModel.databaseName,
            tableMetaModel.tableName,
            this.tableMetaModel.partitions
                .stream()
                .map(p -> p.partitionValues)
                .collect(Collectors.toList()),
            MmaMetaManager.JobStatus.SUCCEEDED);
        MmaTaskSucceedEvent e = new MmaTaskSucceedEvent(id, tableMetaModel.partitions.size());
        MmaEventManager.getInstance().send(e);
      } else if (TaskProgress.FAILED.equals(progress)) {
        MmaTaskFailedEvent e = new MmaTaskFailedEvent(
            id,
            dag.vertexSet()
               .stream()
               .filter(a -> ActionProgress.FAILED.equals(a.getProgress()))
               .map(Action::getName).collect(Collectors.toList()));
        MmaEventManager.getInstance().send(e);

        // Update the status of partition who have passed the verification to SUCCEEDED even when
        // the task failed
        Action verificationAction = null;
        for (Action action : dag.vertexSet()) {
          if (action instanceof VerificationAction) {
            verificationAction = action;
          }
        }

        if (verificationAction != null
            && ActionProgress.FAILED.equals(verificationAction.getProgress())) {
          VerificationActionInfo verificationActionInfo =
              (VerificationActionInfo) verificationAction.getActionInfo();
          mmaMetaManager.updateStatus(
              jobId,
              jobType,
              MmaConfig.ObjectType.TABLE.name(),
              tableMetaModel.databaseName,
              tableMetaModel.tableName,
              verificationActionInfo.getSucceededPartitions(),
              MmaMetaManager.JobStatus.SUCCEEDED);
          mmaMetaManager.updateStatus(
              jobId,
              jobType,
              MmaConfig.ObjectType.TABLE.name(),
              tableMetaModel.databaseName,
              tableMetaModel.tableName,
              verificationActionInfo.getFailedPartitions(),
              MmaMetaManager.JobStatus.FAILED);
        } else {
          mmaMetaManager.updateStatus(
              jobId,
              jobType,
              MmaConfig.ObjectType.TABLE.name(),
              tableMetaModel.databaseName,
              tableMetaModel.tableName,
              tableMetaModel.partitions
                  .stream()
                  .map(p -> p.partitionValues)
                  .collect(Collectors.toList()),
              MmaMetaManager.JobStatus.FAILED);
        }
      }
    } else if (TaskProgress.SUCCEEDED.equals(progress)) {
      MmaTaskSucceedEvent e = new MmaTaskSucceedEvent(id);
      MmaEventManager.getInstance().send(e);

      mmaMetaManager.updateStatus(
          jobId,
          jobType,
          MmaConfig.ObjectType.TABLE.name(),
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          MmaMetaManager.JobStatus.SUCCEEDED);
    } else if (TaskProgress.FAILED.equals(progress)) {
      MmaTaskFailedEvent e = new MmaTaskFailedEvent(
          id,
          dag.vertexSet()
             .stream()
             .filter(a -> ActionProgress.FAILED.equals(a.getProgress()))
             .map(Action::getName)
             .collect(Collectors.toList()));
      MmaEventManager.getInstance().send(e);

      mmaMetaManager.updateStatus(
          jobId,
          jobType,
          MmaConfig.ObjectType.TABLE.name(),
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          MmaMetaManager.JobStatus.FAILED);
    }
  }
}
