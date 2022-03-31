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

package com.aliyun.odps.mma.server.job;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.transform.SchemaTransformer.SchemaTransformResult;
import com.aliyun.odps.mma.meta.transform.SchemaTransformerFactory;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import com.aliyun.odps.mma.server.task.HiveToMcTableDataTransmissionTask;
import com.aliyun.odps.mma.server.task.HiveToMcTableSetUpTask;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;

public class HiveToMcTableJob extends AbstractTableJob {
  private static final Logger LOG = LogManager.getLogger(HiveToMcTableJob.class);

  public HiveToMcTableJob(
      Job parentJob,
      JobRecord record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  DirectedAcyclicGraph<Task, DefaultEdge> generateDag() throws Exception {
    LOG.info("Generate the DAG, job id: {}", record.getJobId());

    try {
      MetaSource metaSource = metaSourceFactory.getMetaSource(config);
      String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
      String tableName = config.get(JobConfiguration.SOURCE_OBJECT_NAME);

      TableMetaModel hiveTableMetaModel = metaSource.getTableMeta(catalogName, tableName);
      SchemaTransformResult schemaTransformResult = SchemaTransformerFactory
          .get(DataSourceType.Hive)
          .transform(hiveTableMetaModel, config);
      TableMetaModel mcTableMetaModel = schemaTransformResult.getTableMetaModel();

      List<Job> pendingSubJobs = null;
      if (!mcTableMetaModel.getPartitionColumns().isEmpty()) {
        pendingSubJobs = jobManager.listSubJobsByStatus(this, JobStatus.PENDING);
      }

      DirectedAcyclicGraph<Task, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
      Task setUpTask = getSetUpTask(
          metaSource,
          hiveTableMetaModel,
          mcTableMetaModel,
          pendingSubJobs);
      List<Task> dataTransmissionTasks = getDataTransmissionTasks(
          metaSource,
          hiveTableMetaModel,
          mcTableMetaModel,
          pendingSubJobs);

      dag.addVertex(setUpTask);
      dataTransmissionTasks.forEach(dag::addVertex);
      dataTransmissionTasks.forEach(t -> dag.addEdge(setUpTask, t));
      return dag;
    } catch (Exception e) {
      String stackTrace = ExceptionUtils.getStackTrace(e);
      fail(stackTrace);
      throw e;
    }
  }

  private Task getSetUpTask(
      MetaSource metaSource,
      TableMetaModel hiveTableMetaModel,
      TableMetaModel mcTableMetaModel,
      List<Job> pendingSubJobs) throws Exception {
    List<TableMetaModel> groups = null;
    if (!hiveTableMetaModel.getPartitionColumns().isEmpty()) {
      groups = getStaticTablePartitionGroups(
          metaSource,
          hiveTableMetaModel,
          mcTableMetaModel,
          pendingSubJobs)
          .stream()
          .map(TablePartitionGroup::getDest)
          .collect(Collectors.toList());
    }
    String taskIdPrefix = generateTaskIdPrefix();
    return new HiveToMcTableSetUpTask(
        taskIdPrefix + ".SetUp",
        getRootJobId(),
        config,
        mcTableMetaModel,
        groups,
        this);
  }

  private List<Task> getDataTransmissionTasks(
      MetaSource metaSource,
      TableMetaModel hiveTableMetaModel,
      TableMetaModel mcTableMetaModel,
      List<Job> pendingSubJobs) throws Exception {
    List<Task> ret = new LinkedList<>();

    boolean isPartitioned = !hiveTableMetaModel.getPartitionColumns().isEmpty();
    String taskIdPrefix = generateTaskIdPrefix();
    String rootJobId = getRootJobId();
    if (isPartitioned) {
      List<TablePartitionGroup> groups = getTablePartitionGroups(
          metaSource,
          hiveTableMetaModel,
          mcTableMetaModel,
          pendingSubJobs);

      for (int i = 0; i < groups.size(); i++) {
        String taskId = taskIdPrefix + ".DataTransmission" + ".part." + i;
        Task task = new HiveToMcTableDataTransmissionTask(
            taskId,
            rootJobId,
            config,
            groups.get(i).getSource(),
            groups.get(i).getDest(),
            this,
            groups.get(i).getJobs());
        LOG.info(
            "Task generated, id: {}, rootJobId: {}, jobs: {}",
            taskId,
            rootJobId,
            groups.get(i).getJobs().stream().map(Job::getId).collect(Collectors.toList()));
        ret.add(task);
      }
    } else {
      Task task = new HiveToMcTableDataTransmissionTask(
          taskIdPrefix + ".DataTransmission",
          rootJobId,
          config,
          hiveTableMetaModel,
          mcTableMetaModel,
          this,
          Collections.emptyList());
      ret.add(task);
    }

    return ret;
  }


  @Override
  public synchronized void setStatus(Task task) {
    if (dag.vertexSet().stream().noneMatch(t -> task.getId().equals(t.getId()))) {
      LOG.info(
          "Outdated task found, job id: {}, task id: {}",
          record.getJobId(),
          task.getId());
      return;
    }

    if (JobStatus.SUCCEEDED.equals(getStatus())
        || JobStatus.FAILED.equals(getStatus())
        || JobStatus.CANCELED.equals(getStatus())) {
      LOG.info("Job has terminated, id: {}, status: {}, task id: {}, task status: {}",
               record.getJobId(),
               getStatus(),
               task.getId(),
               task.getProgress());
      return;
    }

    TaskProgress taskStatus = task.getProgress();

    switch (taskStatus) {
      case SUCCEEDED:
        if (task instanceof HiveToMcTableDataTransmissionTask) {
          handleDataTransmissionTask((HiveToMcTableDataTransmissionTask) task);
        }
        if (dag
            .vertexSet()
            .stream()
            .filter(t -> t instanceof HiveToMcTableDataTransmissionTask)
            .allMatch(t -> TaskProgress.SUCCEEDED.equals(t.getProgress()))) {
          setStatusInternal(JobStatus.SUCCEEDED);
        }
        break;
      case FAILED:
        if (task instanceof HiveToMcTableDataTransmissionTask) {
          handleDataTransmissionTask((HiveToMcTableDataTransmissionTask) task);
        } else {
          String reason = String.format("%s failed, id: %s", task.getClass(), task.getId());
          fail(reason);
        }
        break;
      case RUNNING:
        LOG.info("Job running, id: {}", record.getJobId());
        setStatusInternal(JobStatus.RUNNING);
        break;
      default:
    }
  }
}
