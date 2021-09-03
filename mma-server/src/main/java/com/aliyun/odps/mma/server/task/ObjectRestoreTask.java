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

//import java.util.Objects;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.jgrapht.graph.DefaultEdge;
//import org.jgrapht.graph.DirectedAcyclicGraph;
//
//import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
//import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
//import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
//import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
//
//public class ObjectRestoreTask extends AbstractTask {
//  private static final Logger LOG = LogManager.getLogger(ObjectRestoreTask.class);
//
//  private final String objectType;
//  private final MetaSource.TableMetaModel tableMetaModel;
//
//  public ObjectRestoreTask(
//      String id,
//      String jobId,
//      String objectType,
//      MetaSource.TableMetaModel tableMetaModel,
//      DirectedAcyclicGraph<Action, DefaultEdge> dag,
//      MmaMetaManager mmaMetaManager) {
//    super(id, jobId, dag, mmaMetaManager);
//    this.objectType = objectType;
//    this.tableMetaModel = Objects.requireNonNull(tableMetaModel);
//    this.actionExecutionContext.setTableMetaModel(tableMetaModel);
//  }
//
//  @Override
//  void updateMetadata() throws MmaException {
//    if (TaskProgress.PENDING.equals(progress)
//        || TaskProgress.RUNNING.equals(progress)) {
//      return;
//    }
//    MmaMetaManager.JobStatus status = TaskProgress.SUCCEEDED.equals(progress) ?
//        MmaMetaManager.JobStatus.SUCCEEDED : MmaMetaManager.JobStatus.FAILED;
//
//    if (MmaConfig.ObjectType.DATABASE.name().equalsIgnoreCase(objectType)) {
//      mmaMetaManager.updateStatus(
//          jobId,
//          MmaConfig.JobType.RESTORE.name(),
//          objectType,
//          tableMetaModel.databaseName,
//          "",
//          status);
//    }
//    else {
//      this.mmaMetaManager.updateStatusInRestoreDB(
//          jobId,
//          MmaConfig.JobType.RESTORE.name(),
//          objectType,
//          tableMetaModel.databaseName,
//          tableMetaModel.tableName,
//          status);
//    }
//  }
//}
