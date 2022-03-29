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
import com.aliyun.odps.mma.server.task.McToOssTableCleanUpTask;
import com.aliyun.odps.mma.server.task.McToOssTableDataTransmissionTask;
import com.aliyun.odps.mma.server.task.McToOssTableMetadataTransmissionTask;
import com.aliyun.odps.mma.server.task.McToOssTableSetUpTask;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;
import com.aliyun.odps.mma.util.McSqlUtils;

public class McToOssTableJob extends AbstractTableJob {

  private static final Logger LOG = LogManager.getLogger(McToOssTableJob.class);
  private TableMetaModel mcExternalTableMetaModel;
  private boolean cleaned = false;

  public McToOssTableJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
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
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ENDPOINT_INTERNAL),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ENDPOINT_EXTERNAL),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_BUCKET),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ROLE_ARN),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ACCESS_KEY_ID),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ACCESS_KEY_SECRET));

      String[] locations = OssUtils.getOssPaths(
          config.get(AbstractConfiguration.METADATA_DEST_OSS_PATH),
          config.get(JobConfiguration.JOB_ID),
          config.get(JobConfiguration.OBJECT_TYPE),
          config.get(JobConfiguration.DEST_CATALOG_NAME),
          config.get(JobConfiguration.DEST_OBJECT_NAME));
      String metadataLocation = locations[0];
      String dataLocation = locations[1];

      MetaSource metaSource = metaSourceFactory.getMetaSource(config);

      // Difference between mcTableMetaModel, ossTableMetaModel and mcExternalTableMetaModel
      // model        catalog           table             location
      // mc           source catalog    source table
      // oss          dest catalog      dest table        oss !metadata location
      // external     source catalog    source table      ak + oss !data location
      TableMetaModel mcTableMetaModel = metaSource.getTableMeta(
          config.get(JobConfiguration.SOURCE_CATALOG_NAME),
          config.get(JobConfiguration.SOURCE_OBJECT_NAME));

      SchemaTransformResult schemaTransformResult = SchemaTransformerFactory
          .get(DataSourceType.MaxCompute)
          .transform(mcTableMetaModel, config);
      TableMetaModelBuilder ossTableMetaModelBuilder =
          new TableMetaModelBuilder(schemaTransformResult.getTableMetaModel());
      ossTableMetaModelBuilder.database(config.get(JobConfiguration.DEST_CATALOG_NAME))
                              .table(config.get(JobConfiguration.DEST_OBJECT_NAME))
                              .location(metadataLocation);
      TableMetaModel ossTableMetaModel = ossTableMetaModelBuilder.build();

      mcExternalTableMetaModel =
          McSqlUtils.getMcExternalTableMetaModel(mcTableMetaModel, ossConfig, dataLocation, getRootJobId());

      List<Job> pendingSubJobs = null;
      if (!mcTableMetaModel.getPartitionColumns().isEmpty()) {
        pendingSubJobs = jobManager.listSubJobsByStatus(this, JobStatus.PENDING);
      }

      DirectedAcyclicGraph<Task, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

      // ossTableMetaModel is for metadata transmission task
      // mcExternalTableMetaModel is for data transmission tasks
      Task metadataTransmissionTask = getMetadataTransmissionTask(ossTableMetaModel, ossConfig);
      Task setUpTask = getSetUpTask(
          metaSource,
          mcTableMetaModel,
          mcExternalTableMetaModel,
          pendingSubJobs);
      List<Task> dataTransmissionTasks = getDataTransmissionTasks(
          metaSource,
          mcTableMetaModel,
          mcExternalTableMetaModel,
          pendingSubJobs);
      Task cleanUpTask = getCleanUpTask(mcExternalTableMetaModel);

      dag.addVertex(metadataTransmissionTask);
      dag.addVertex(setUpTask);
      dataTransmissionTasks.forEach(dag::addVertex);
      dag.addVertex(cleanUpTask);

      dag.addEdge(metadataTransmissionTask, setUpTask);
      dataTransmissionTasks.forEach(t -> dag.addEdge(setUpTask, t));
      dataTransmissionTasks.forEach(t -> dag.addEdge(t, cleanUpTask));

      return dag;
    } catch (Exception e) {
      String stackTrace = ExceptionUtils.getStackTrace(e);
      fail(stackTrace);
      throw e;
    }
  }

  private Task getMetadataTransmissionTask(TableMetaModel tableMetaModel, OssConfig ossConfig) {
    String taskIdPrefix = generateTaskIdPrefix();
    return new McToOssTableMetadataTransmissionTask(
        taskIdPrefix + ".MetadataTransmission",
        getRootJobId(),
        config,
        ossConfig,
        tableMetaModel,
        this);
  }

  private Task getSetUpTask(
      MetaSource metaSource,
      TableMetaModel mcTableMetaModel,
      TableMetaModel mcExternalMetaModel,
      List<Job> pendingSubJobs) throws Exception {
    List<TableMetaModel> groupDests = null;
    if (!mcTableMetaModel.getPartitionColumns().isEmpty()) {
      groupDests = getStaticTablePartitionGroups(
          metaSource,
          mcTableMetaModel,
          mcExternalMetaModel,
          pendingSubJobs)
          .stream()
          .map(TablePartitionGroup::getDest)
          .collect(Collectors.toList());
    }
    String taskIdPrefix = generateTaskIdPrefix();
    return new McToOssTableSetUpTask(
        taskIdPrefix + ".SetUp",
        getRootJobId(),
        config,
        mcExternalMetaModel,
        groupDests,
        this);
  }

  private List<Task> getDataTransmissionTasks(
      MetaSource metaSource,
      TableMetaModel mcTableMetaModel,
      TableMetaModel mcExternalMetaModel,
      List<Job> pendingSubJobs) throws Exception {
    List<Task> ret = new LinkedList<>();

    boolean isPartitioned = !mcTableMetaModel.getPartitionColumns().isEmpty();
    String taskIdPrefix = generateTaskIdPrefix();
    String rootJobId = getRootJobId();
    if (isPartitioned) {
      List<TablePartitionGroup> groups =
          getTablePartitionGroups(metaSource, mcTableMetaModel, mcExternalMetaModel, pendingSubJobs);
      for (int i = 0; i < groups.size(); i++) {
        String taskId = taskIdPrefix + ".DataTransmission" + ".part." + i;
        Task task = new McToOssTableDataTransmissionTask(
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
      Task task = new McToOssTableDataTransmissionTask(
          taskIdPrefix + ".DataTransmission",
          rootJobId,
          config,
          mcTableMetaModel,
          mcExternalMetaModel,
          this,
          Collections.emptyList());
      ret.add(task);
    }

    return ret;
  }

  private Task getCleanUpTask(TableMetaModel tableMetaModel) {
    String taskIdPrefix = generateTaskIdPrefix();
    return new McToOssTableCleanUpTask(
        taskIdPrefix + ".CleanUp",
        getRootJobId(),
        config,
        tableMetaModel,
        this);
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
        if (task instanceof McToOssTableDataTransmissionTask) {
          handleDataTransmissionTask((McToOssTableDataTransmissionTask) task);
        } else if (task instanceof McToOssTableCleanUpTask) {
          LOG.info("Job succeeded, id: {}", record.getJobId());
          setStatusInternal(JobStatus.SUCCEEDED);
        }
        break;
      case FAILED:
        if (task instanceof McToOssTableDataTransmissionTask) {
          handleDataTransmissionTask((McToOssTableDataTransmissionTask) task);
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
    this.dag.addVertex(getCleanUpTask(mcExternalTableMetaModel));
    cleaned = true;
    return false;
  }

  @Override
  public synchronized boolean reset(boolean force) throws Exception {
    cleaned = false;
    return super.reset(force);
  }

}
