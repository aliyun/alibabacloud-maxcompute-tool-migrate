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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.transform.SchemaTransformer.SchemaTransformResult;
import com.aliyun.odps.mma.meta.transform.SchemaTransformerFactory;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel.TableMetaModelBuilder;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import com.aliyun.odps.mma.server.task.OssToMcTableCleanUpTask;
import com.aliyun.odps.mma.server.task.OssToMcTableDataTransmissionTask;
import com.aliyun.odps.mma.server.task.OssToMcTableSetUpTask;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;
import com.aliyun.odps.mma.util.McSqlUtils;

public class OssToMcTableJob extends AbstractTableJob {

  private static final Logger LOG = LogManager.getLogger(OssToMcTableJob.class);
  private TableMetaModel externalTableMetaModel;
  private boolean cleaned = false;

  OssToMcTableJob(
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
      OssConfig ossConfig = new OssConfig(
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_INTERNAL),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_EXTERNAL),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_BUCKET),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ROLE_ARN),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_ID),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_SECRET));
      String[] locations = OssUtils.getOssPaths(
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_PATH),
          config.get(JobConfiguration.JOB_ID),
          config.get(JobConfiguration.OBJECT_TYPE),
          config.get(JobConfiguration.SOURCE_CATALOG_NAME),
          config.get(JobConfiguration.SOURCE_OBJECT_NAME));
      String metadataLocation = locations[0];
      String dataLocation = locations[1];

      MetaSource metaSource = metaSourceFactory.getMetaSource(config);

      // Difference between ossTableMetaModel, mcTableMetaModel and mcExternalTableMetaModel
      // model        catalog           table             location
      // oss          source catalog    source table      oss !metadata location
      // mc           dest catalog      dest table
      // external     dest catalog      dest table        ak + oss !data location
      TableMetaModel ossTableMetaModel = metaSource.getTableMeta(
          config.get(JobConfiguration.SOURCE_CATALOG_NAME),
          config.get(JobConfiguration.SOURCE_OBJECT_NAME));

      SchemaTransformResult schemaTransformResult = SchemaTransformerFactory
          .get(DataSourceType.OSS)
          .transform(ossTableMetaModel, config);
      TableMetaModelBuilder builder =
          new TableMetaModelBuilder(schemaTransformResult.getTableMetaModel());
      TableMetaModel mcTableMetaModel =
          builder.database(config.get(JobConfiguration.DEST_CATALOG_NAME))
              .table(config.get(JobConfiguration.DEST_OBJECT_NAME))
              .build();

      externalTableMetaModel =
          McSqlUtils.getMcExternalTableMetaModel(mcTableMetaModel, ossConfig, dataLocation, getRootJobId());

      // for local debug
      // OssUtils.getTableModelLogInfo(mcTableMetaModel, ossTableMetaModel, externalTableMetaModel);

      List<Job> pendingSubJobs = null;
      if (!ossTableMetaModel.getPartitionColumns().isEmpty()) {
        pendingSubJobs = jobManager.listSubJobsByStatus(this, JobStatus.PENDING);
      }

      DirectedAcyclicGraph<Task, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
      Task setUpTask = getSetUpTask(
          metaSource,
          ossTableMetaModel,
          mcTableMetaModel,
          externalTableMetaModel,
          pendingSubJobs);
      List<Task> dataTransmissionTasks = getDataTransmissionTasks(
          metaSource,
          ossTableMetaModel,
          mcTableMetaModel,
          externalTableMetaModel,
          pendingSubJobs);
      Task cleanUpTask = getCleanUpTask(externalTableMetaModel);
      dag.addVertex(setUpTask);
      dataTransmissionTasks.forEach(dag::addVertex);
      dag.addVertex(cleanUpTask);

      dataTransmissionTasks.forEach(t -> dag.addEdge(setUpTask, t));
      dataTransmissionTasks.forEach(t -> dag.addEdge(t, cleanUpTask));
      if (dataTransmissionTasks.isEmpty()) {
        dag.addEdge(setUpTask, cleanUpTask);
      }
      return dag;
    } catch (Exception e) {
      String stackTrace = ExceptionUtils.getStackTrace(e);
      fail(stackTrace);
      throw e;
    }
  }

  private Task getSetUpTask(
      MetaSource metaSource,
      TableMetaModel ossTableMetaModel,
      TableMetaModel mcTableMetaModel,
      TableMetaModel mcExternalTableMetaModel,
      List<Job> pendingSubJobs) throws Exception {
    List<TablePartitionGroup> groups = null;
    if (!ossTableMetaModel.getPartitionColumns().isEmpty()) {
      groups = getTablePartitionGroups(metaSource,
                                       ossTableMetaModel,
                                       mcTableMetaModel,
                                       mcExternalTableMetaModel,
                                       pendingSubJobs);
    }
    String taskIdPrefix = generateTaskIdPrefix();
    return new OssToMcTableSetUpTask(
        taskIdPrefix + ".SetUp",
        getRootJobId(),
        config,
        mcExternalTableMetaModel,
        mcTableMetaModel,
        groups,
        this);
  }

  private List<Task> getDataTransmissionTasks(
      MetaSource metaSource,
      TableMetaModel ossTableMetaModel,
      TableMetaModel mcTableMetaModel,
      TableMetaModel externalTableMetaModel,
      List<Job> pendingSubJobs) throws Exception {
    List<Task> ret = new LinkedList<>();

    boolean isPartitioned = !ossTableMetaModel.getPartitionColumns().isEmpty();
    String taskIdPrefix = generateTaskIdPrefix();
    String rootJobId = getRootJobId();
    if (isPartitioned) {
      // External table's metadata doesn't contain partition size. So the adaptive way won't work.
      List<TablePartitionGroup> groups = getTablePartitionGroups(metaSource,
                                                                 ossTableMetaModel,
                                                                 mcTableMetaModel,
                                                                 externalTableMetaModel,
                                                                 pendingSubJobs);

      for (int i = 0; i < groups.size(); i++) {
        String taskId = taskIdPrefix + ".DataTransmission" + ".part." + i;
        Task task = new OssToMcTableDataTransmissionTask(
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
      Task task = new OssToMcTableDataTransmissionTask(
          taskIdPrefix + ".DataTransmission",
          rootJobId,
          config,
          externalTableMetaModel,
          mcTableMetaModel,
          this,
          Collections.emptyList());
      ret.add(task);
    }

    return ret;
  }

  private Task getCleanUpTask(TableMetaModel mcExternalTableMetaModel) {
    String taskIdPrefix = generateTaskIdPrefix();
    return new OssToMcTableCleanUpTask(
        taskIdPrefix + ".CleanUp",
        getRootJobId(),
        config,
        mcExternalTableMetaModel,
        this);
  }

  private List<TablePartitionGroup> getTablePartitionGroups(
      MetaSource metaSource,
      TableMetaModel ossTableMetaModel,
      TableMetaModel mcTableMetaModel,
      TableMetaModel mcExternalMetaModel,
      List<Job> pendingSubJobs
  ) throws Exception {
    // 1. get        pt group    (source: ossModel,      mcModel) by ossTableMetaModel
    // 2. transform  pt group to (source: externalModel, mcModel)
    List<TablePartitionGroup> groups = getStaticTablePartitionGroups(
        metaSource,
        ossTableMetaModel,
        mcTableMetaModel,
        pendingSubJobs);
    List<TablePartitionGroup> newGroups = new ArrayList<>();
    for (TablePartitionGroup group : groups) {
      TableMetaModel source = group.getSource();
      TableMetaModelBuilder builder = new TableMetaModelBuilder(source);
      builder.database(mcExternalMetaModel.getDatabase())
          .table(mcExternalMetaModel.getTable())
          .location(mcExternalMetaModel.getLocation());
      newGroups.add(new TablePartitionGroup(builder.build(), group.getDest(), group.getJobs()));
    }
    return newGroups;
  }

  @Override
  public synchronized void setStatus(Task task) {
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
        if (task instanceof OssToMcTableDataTransmissionTask) {
          handleDataTransmissionTask((OssToMcTableDataTransmissionTask) task);
        } else if (task instanceof OssToMcTableCleanUpTask) {
          LOG.info("Job succeeded, id: {}", record.getJobId());
          setStatusInternal(JobStatus.SUCCEEDED);
        }
        break;
      case FAILED:
        if (task instanceof OssToMcTableDataTransmissionTask) {
          handleDataTransmissionTask((OssToMcTableDataTransmissionTask) task);
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

  @Override
  public boolean clean() {
    if (cleaned) {
      return true;
    }
    this.dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    this.dag.addVertex(getCleanUpTask(externalTableMetaModel));
    cleaned = true;
    return false;
  }

  @Override
  public synchronized boolean reset(boolean force) throws Exception {
    cleaned = false;
    return super.reset(force);
  }
}
