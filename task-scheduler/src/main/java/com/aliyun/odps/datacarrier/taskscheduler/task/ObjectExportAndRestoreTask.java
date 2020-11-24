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

import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.RestoreTaskInfo;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;

public class ObjectExportAndRestoreTask extends AbstractTask {
  private static final Logger LOG = LogManager.getLogger(ObjectExportAndRestoreTask.class);

  private final MetaSource.TableMetaModel tableMetaModel;
  private RestoreTaskInfo restoreTaskInfo;

  public ObjectExportAndRestoreTask(String id,
                                    MetaSource.TableMetaModel tableMetaModel,
                                    DirectedAcyclicGraph<Action, DefaultEdge> dag,
                                    MmaMetaManager mmaMetaManager) {
    super(id, dag, mmaMetaManager);
    this.tableMetaModel = Objects.requireNonNull(tableMetaModel);
    actionExecutionContext.setTableMetaModel(this.tableMetaModel);
  }

  public void setRestoreTaskInfo(RestoreTaskInfo restoreTaskInfo) {
    this.restoreTaskInfo = restoreTaskInfo;
  }

  public RestoreTaskInfo getRestoreTaskInfo() {
    return restoreTaskInfo;
  }

  @Override
  void updateMetadata() throws MmaException {
    if (TaskProgress.PENDING.equals(progress) || TaskProgress.RUNNING.equals(progress)) {
      return;
    }
    MmaMetaManager.MigrationStatus status = TaskProgress.SUCCEEDED.equals(progress) ?
        MmaMetaManager.MigrationStatus.SUCCEEDED :
        MmaMetaManager.MigrationStatus.FAILED;
    if (restoreTaskInfo != null) {
      mmaMetaManager.updateStatusInRestoreDB(restoreTaskInfo, status);
    } else {
      mmaMetaManager.updateStatus(tableMetaModel.databaseName,
          tableMetaModel.tableName,
          status);
    }
  }
}
