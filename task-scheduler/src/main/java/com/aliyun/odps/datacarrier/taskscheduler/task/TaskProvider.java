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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.Function;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.datacarrier.taskscheduler.BackgroundLoopManager;
import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
import com.aliyun.odps.datacarrier.taskscheduler.DropRestoredTemporaryTableWorkItem;
import com.aliyun.odps.datacarrier.taskscheduler.ExternalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.ExternalTableStorage;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfigUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.AddBackgroundWorkItemAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.AddMigrationJobAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.HiveSourceVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.HiveUdtfDataTransferAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsAddPartitionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsBackupTableDdlAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsCreateOssExternalTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsCreateTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDatabaseRestoreAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDestVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDropPartitionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDropTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportFunctionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportResourceAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportViewAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsInsertOverwriteDataTransferAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsResetTableMetaModelAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreFunctionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestorePartitionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreResourceAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsSourceVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.VerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.JobInfo;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.annotations.VisibleForTesting;

public class TaskProvider {
  private static final Logger LOG = LogManager.getLogger(TaskProvider.class);

  private MmaMetaManager mmaMetaManager;
  private BackgroundLoopManager backgroundLoopManager;
  private List<Task> pendingTasks = new ArrayList<>();

  public TaskProvider(MmaMetaManager mmaMetaManager) {
    this.mmaMetaManager = Objects.requireNonNull(mmaMetaManager);
    backgroundLoopManager = new BackgroundLoopManager();
    backgroundLoopManager.start();
  }

  public synchronized List<Task> get() throws MmaException {
    List<Task> ret = new LinkedList<>();
    ret.addAll(getTasksFromMetaDB());
    ret.addAll(getTasksFromRestoreDB());
    synchronized (pendingTasks) {
      ret.addAll(pendingTasks);
      pendingTasks.clear();
    }
    return ret;
  }

  public List<Task> getTasksFromMetaDB() throws MmaException {
    List<JobInfo> pendingTables = mmaMetaManager.getPendingJobs();
    if (!pendingTables.isEmpty()) {
      for (JobInfo jobInfo : pendingTables) {
        LOG.info("Found pending job, UniqueId: {}, JobType: {}, ObjectType: {}, Database: {}, Object: {}",
                 jobInfo.getJobId(),
                 jobInfo.getJobType(),
                 jobInfo.getObjectType(),
                 jobInfo.getDb(),
                 jobInfo.getObject());
      }
    } else {
      LOG.info("No pending jobs");
    }

    setRunning(pendingTables);

    List<Task> ret = new LinkedList<>();

    for (JobInfo jobInfo : pendingTables) {
      MetaSource.TableMetaModel tableMetaModel = jobInfo.getTableMetaModel();
      MmaConfig.JobConfig config = jobInfo.getJobConfig();
      MmaConfig.JobType jobType = jobInfo.getJobType();

      LOG.info("Job type: {}, object type: {}, db: {}, object: {}",
               jobType,
               jobInfo.getObjectType(),
               jobInfo.getDb(),
               jobInfo.getObject());

      String taskIdPrefix = getTaskIdPrefix(
          jobType,
          jobInfo.getObjectType(),
          jobInfo.getDb(),
          jobInfo.getObject());

      if (MmaConfig.JobType.MIGRATION.equals(jobType)) {
        MmaConfig.TableMigrationConfig tableMigrationConfig =
            MmaConfig.TableMigrationConfig.fromJson(config.getDescription());
        if (tableMetaModel.partitionColumns.isEmpty()) {
          Task task = generateNonPartitionedTableMigrationTask(
              jobInfo.getJobId(),
              taskIdPrefix,
              DataSource.ODPS,
              tableMetaModel,
              tableMigrationConfig);
          ((AbstractTask)task).setOdpsConfig(tableMigrationConfig.getOdpsConfig());
          ret.add(task);
          continue;
        } else {
          List<Task> tasks = generatePartitionedTableMigrationTask(
              jobInfo.getJobId(),
              taskIdPrefix,
              DataSource.ODPS,
              tableMetaModel,
              tableMigrationConfig);

          tasks.forEach(
              t -> ((AbstractTask) t).setOdpsConfig(tableMigrationConfig.getOdpsConfig()));
          ret.addAll(tasks);
        }
      } else if (MmaConfig.JobType.BACKUP.equals(jobType)) {
        List<Task> tasks;
        MmaConfig.ObjectBackupConfig backupConfig =
            MmaConfig.ObjectBackupConfig.fromJson(config.getDescription());
        String backupName = backupConfig.getBackupName();
        Task task = null;
        switch (backupConfig.getObjectType()) {
          case TABLE:
            tasks = generateTableBackupTasks(
                jobInfo.getJobId(),
                taskIdPrefix,
                backupName,
                tableMetaModel,
                backupConfig,
                config.getAdditionalTableConfig());
            tasks.forEach(t -> {
              ((AbstractTask)t).setOdpsConfig(backupConfig.getOdpsConfig());
              ((AbstractTask)t).setOssConfig(backupConfig.getOssConfig());
            });
            ret.addAll(tasks);
            break;
          case FUNCTION:
            task = generateFunctionBackupTask(
                jobInfo.getJobId(),
                taskIdPrefix,
                backupConfig,
                tableMetaModel);
            break;
          case RESOURCE:
            task = generateResourceBackupTask(
                jobInfo.getJobId(),
                taskIdPrefix,
                backupConfig,
                tableMetaModel);
            break;
          default:
            LOG.error("Unsupported object type {} when backup {}.{} backup name {} to OSS",
                      backupConfig.getObjectType(),
                      backupConfig.getDatabaseName(),
                      backupConfig.getObjectName(),
                      backupConfig.getBackupName());
            break;
        }
        if (task != null) {
          ((AbstractTask)task).setOdpsConfig(backupConfig.getOdpsConfig());
          ((AbstractTask)task).setOssConfig(backupConfig.getOssConfig());
          ret.add(task);
        }
      } else if (MmaConfig.JobType.RESTORE.equals(jobType)) {
        Task task;
        if (MmaConfig.ObjectType.DATABASE.equals(jobInfo.getObjectType())) {
          MmaConfig.DatabaseRestoreConfig restoreConfig =
              MmaConfig.DatabaseRestoreConfig.fromJson(config.getDescription());
          task = generateDatabaseRestoreTask(
              jobInfo.getJobId(),
              taskIdPrefix,
              restoreConfig,
              tableMetaModel);
          ((AbstractTask)task).setOdpsConfig(restoreConfig.getOdpsConfig());
          ((AbstractTask)task).setOssConfig(restoreConfig.getOssConfig());
        } else {
          LOG.warn("Object restore job not supported");
          continue;
        }
        ret.add(task);
      }
      LOG.warn("Unsupported job type {} for {}.{}",
               jobType,
               tableMetaModel.databaseName,
               tableMetaModel.tableName);
      // TODO: should mark corresponding job as failed
    }

    return ret;
  }

  public List<Task> getTasksFromRestoreDB() throws MmaException {
    List<Task> ret = new LinkedList<Task>();
    String condition = "WHERE status='PENDING'\n";
    List<MmaMetaManagerDbImplUtils.RestoreJobInfo> jobs = this.mmaMetaManager.listRestoreJobs(condition, 100);
    if (jobs.isEmpty()) {
      LOG.info("No pending object restore job found.");
      return ret;
    }
    for (MmaMetaManagerDbImplUtils.RestoreJobInfo jobInfo : jobs) {

      MmaConfig.ObjectRestoreConfig config = MmaConfig.ObjectRestoreConfig.fromJson(jobInfo.getJobConfig().getDescription());
      MetaSource.TableMetaModel tableMetaModel = new MetaSource.TableMetaModel();
      tableMetaModel.databaseName = config.getSourceDatabaseName().toLowerCase();
      tableMetaModel.odpsProjectName = config.getDestinationDatabaseName().toLowerCase();
      tableMetaModel.tableName = config.getObjectName().toLowerCase();
      tableMetaModel.odpsTableName = config.getObjectName().toLowerCase();

      String taskIdPrefix = getTaskIdPrefix(MmaConfig.JobType.RESTORE, jobInfo

          .getObjectType(), jobInfo
                                                .getDb(), jobInfo
                                                .getObject());
      Task task = generateObjectRestoreTask(jobInfo
                                                .getJobId(), taskIdPrefix, config, tableMetaModel);



      if (task != null) {
        ((AbstractTask)task).setOdpsConfig(config.getOdpsConfig());
        ((AbstractTask)task).setOssConfig(config.getOssConfig());
        jobInfo.setStatus(MmaMetaManager.JobStatus.RUNNING);
        jobInfo.setLastModifiedTime(System.currentTimeMillis());
        this.mmaMetaManager.mergeJobInfoIntoRestoreDB(jobInfo);
        ret.add(task);
      }
    }
    return ret;
  }

  public void addPendingTask(Task task) {
    synchronized (pendingTasks) {
      pendingTasks.add(task);
    }
  }

  private void setRunning(List<JobInfo> pendingTables) throws MmaException {
    for (JobInfo jobInfo : pendingTables) {
      MetaSource.TableMetaModel tableMetaModel = jobInfo.getTableMetaModel();

      if (tableMetaModel != null
          && !tableMetaModel.partitionColumns.isEmpty()
          && !tableMetaModel.partitions.isEmpty()) {
        mmaMetaManager.updateStatus(
            jobInfo.getJobId(),
            jobInfo.getJobType().name(),
            jobInfo.getObjectType().name(),
            tableMetaModel.databaseName,
            tableMetaModel.tableName,
            tableMetaModel.partitions
                .stream()
                .map(p -> p.partitionValues)
                .collect(Collectors.toList()),
            MmaMetaManager.JobStatus.RUNNING);
      }
      mmaMetaManager.updateStatus(
          jobInfo.getJobId(),
          jobInfo.getJobType().name(),
          jobInfo.getObjectType().name(),
          jobInfo.getDb(),
          jobInfo.getObject(),
          MmaMetaManager.JobStatus.RUNNING);
    }
  }

  private Task generateNonPartitionedTableMigrationTask(
      String jobId,
      String taskIdPrefix,
      DataSource datasource,
      MetaSource.TableMetaModel tableMetaModel,
      MmaConfig.TableMigrationConfig config) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag;
    switch (datasource) {
      case Hive:
        dag = getHiveNonPartitionedTableMigrationActionDag(taskIdPrefix);
        break;
      case ODPS: {
        String destTableStorage = config.getDestTableStorage();
        if (!StringUtils.isNullOrEmpty(destTableStorage)) {
          ExternalTableStorage storage = ExternalTableStorage.valueOf(destTableStorage);
          String location = null;
          if (ExternalTableStorage.OSS.equals(storage)) {
            MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
            String ossFolder =
                tableMetaModel.odpsProjectName + ".db/" + tableMetaModel.odpsTableName + "/";
            location = OdpsSqlUtils.getOssTablePath(ossConfig, ossFolder);
          }
          ExternalTableConfig externalTableConfig = new ExternalTableConfig(storage, location);
          dag = getOdpsNonPartitionedTableMigrationActionDag(
              taskIdPrefix, true, externalTableConfig);
        } else {
          dag = getOdpsNonPartitionedTableMigrationActionDag(taskIdPrefix);
        }
        break;
      }
      default:
        throw new UnsupportedOperationException();
    }

    return new MigrationTask(
        taskIdPrefix,
        jobId,
        MmaConfig.JobType.MIGRATION.name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private List<Task> generatePartitionedTableMigrationTask(
      String jobId,
      String taskIdPrefix,
      DataSource datasource,
      MetaSource.TableMetaModel tableMetaModel,
      MmaConfig.TableMigrationConfig config) {
    List<Task> ret = new LinkedList<>();
    if (tableMetaModel.partitions.isEmpty()) {
      OdpsCreateTableAction createTableAction = new OdpsCreateTableAction(
          taskIdPrefix + ".CreateTable");
      DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
      dag.addVertex(createTableAction);
      Task task = new MigrationTask(
          taskIdPrefix,
          jobId,
          MmaConfig.JobType.MIGRATION.name(),
          tableMetaModel,
          dag,
          mmaMetaManager);
      ret.add(task);
      return ret;
    }

    List<MetaSource.TableMetaModel> tableSplits =
        getTableSplits(tableMetaModel, config.getAdditionalTableConfig());

    for (int i = 0; i < tableSplits.size(); i++) {
      MetaSource.TableMetaModel split = tableSplits.get(i);
      String taskId = taskIdPrefix + "-part" + i;

      DirectedAcyclicGraph<Action, DefaultEdge> dag;
      switch (datasource) {
        case OSS: {
          dag = getHivePartitionedTableMigrationActionDag(taskId);
          break;
        }
        case ODPS: {
          String destTableStorage = config.getDestTableStorage();
          if (!StringUtils.isNullOrEmpty(destTableStorage)) {
            ExternalTableStorage storage = ExternalTableStorage.valueOf(destTableStorage);
            String location = null;
            if (ExternalTableStorage.OSS.equals(storage)) {
              MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
              String ossFolder =
                  tableMetaModel.odpsProjectName + ".db/" + tableMetaModel.odpsTableName + "/";
              location = OdpsSqlUtils.getOssTablePath(ossConfig, ossFolder);
            }
            ExternalTableConfig externalTableConfig = new ExternalTableConfig(storage, location);
            dag = getOdpsPartitionedTableMigrationActionDag(
                taskId,
                true,
                externalTableConfig);
          } else {
            dag = getOdpsPartitionedTableMigrationActionDag(taskId);
          }
          break;
        }
        default:
          throw new UnsupportedOperationException();
      }

      ret.add(new MigrationTask(
          taskId,
          jobId,
          MmaConfig.JobType.MIGRATION.name(),
          split,
          dag,
          mmaMetaManager));
    }

    return ret;
  }

  private List<Task> generateTableBackupTasks(String jobId, String taskIdPrefix, String backupName, MetaSource.TableMetaModel tableMetaModel, MmaConfig.ObjectBackupConfig backupConfig, MmaConfig.AdditionalTableConfig additionalTableConfig) {
    List<MetaSource.TableMetaModel> splitResult =
        getTableSplits(tableMetaModel, additionalTableConfig);
    String location = OssUtils.getOssPathToExportObject(
        backupName,
        "tables/",
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        Constants.EXPORT_TABLE_DATA_FOLDER);
    MmaConfig.OssConfig ossConfig = backupConfig.getOssConfig();
    ExternalTableConfig externalTableConfig = new ExternalTableConfig(
        ExternalTableStorage.OSS,
        OdpsSqlUtils.getOssTablePath(ossConfig, location));

    List<Task> tasks = new ArrayList<>();
    AtomicInteger lineageTasksCounter = new AtomicInteger(0);
    for (int index = 0; index < splitResult.size(); index++) {
      DirectedAcyclicGraph<Action, DefaultEdge> dag;
      MetaSource.TableMetaModel model = splitResult.get(index);
      String taskId = taskIdPrefix + ((splitResult.size() == 1) ? "" : ("." + index));

      if (tableMetaModel.partitionColumns.isEmpty()) {
        dag = getOdpsNonPartitionedTableMigrationActionDag(
            taskId, true, externalTableConfig);
      }
      else {
        dag = getOdpsPartitionedTableMigrationActionDag(
            taskId, true, externalTableConfig);
      }

      Action leafAction = null;
      for (Action action : dag.vertexSet()) {
        if (dag.getDescendants(action).isEmpty()) {
          leafAction = action;
          break;
        }
      }
      OdpsBackupTableDdlAction exportTableDdlAction = new OdpsBackupTableDdlAction(
          taskId + ".ExportTableDDL", backupName, tableMetaModel, lineageTasksCounter);
      dag.addVertex(exportTableDdlAction);
      dag.addEdge(leafAction, exportTableDdlAction);
      MigrationTask task = new MigrationTask(
          taskId,
          jobId,
          MmaConfig.JobType.BACKUP.name(),
          model,
          dag,
          mmaMetaManager);
      tasks.add(task);
    }
    return tasks;
  }

  private Task generateViewExportTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectBackupConfig backupConfig,
      MetaSource.TableMetaModel tableMetaModel) {
    Table table = OdpsUtils.getTable(
        backupConfig.getOdpsConfig(),
        tableMetaModel.databaseName,
        tableMetaModel.tableName);
    if (table == null) {
      LOG.warn("Table {}.{} not found",
               tableMetaModel.databaseName, tableMetaModel.tableName);
      return null;
    }
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsExportViewAction action = new OdpsExportViewAction(
        taskIdPrefix + ".ExportViewDDL", backupConfig.getBackupName(), table.getViewText());
    dag.addVertex(action);
    return new ObjectBackupTask(
        taskIdPrefix,
        jobId,
        backupConfig.getObjectType().name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private Task generateFunctionBackupTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectBackupConfig backupConfig,
      MetaSource.TableMetaModel tableMetaModel) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    Function function = OdpsUtils.getFunction(
        backupConfig.getOdpsConfig(),
        tableMetaModel.databaseName,
        tableMetaModel.tableName);
    if (function == null) {
      return null;
    }
    OdpsExportFunctionAction action = new OdpsExportFunctionAction(
        taskIdPrefix + ".ExportFunctionDDL", backupConfig.getBackupName(), function);
    dag.addVertex(action);
    return new ObjectBackupTask(
        taskIdPrefix,
        jobId,
        backupConfig.getObjectType().name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private Task generateResourceBackupTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectBackupConfig exportConfig,
      MetaSource.TableMetaModel tableMetaModel) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    Resource resource = OdpsUtils.getResource(
        exportConfig.getOdpsConfig(),
        tableMetaModel.databaseName,
        tableMetaModel.tableName);
    if (resource == null) {
      return null;
    }
    OdpsExportResourceAction action = new OdpsExportResourceAction(
        taskIdPrefix + ".ExportResourceDDL",
        exportConfig.getBackupName(),
        resource);
    dag.addVertex(action);
    return new ObjectBackupTask(
        taskIdPrefix,
        jobId,
        exportConfig.getObjectType().name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private Task generateObjectRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectRestoreConfig restoreConfig,
      MetaSource.TableMetaModel tableMetaModel) {
    Task task = null;
    switch (restoreConfig.getObjectType()) {
      case FUNCTION:
        return generateFunctionRestoreTask(jobId, taskIdPrefix, restoreConfig, tableMetaModel);
      case RESOURCE:
        return generateResourceRestoreTask(jobId, taskIdPrefix, restoreConfig, tableMetaModel);
      case TABLE:
        return generateTableRestoreTask(jobId, taskIdPrefix, restoreConfig, tableMetaModel);
      case VIEW:
        return generateViewRestoreTask(jobId, taskIdPrefix, restoreConfig, tableMetaModel);
    }

    LOG.error("Unsupported restore type {}",
              MmaConfig.ObjectRestoreConfig.toJson(restoreConfig));

    return task;
  }

  private Task generateTableRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectRestoreConfig restoreConfig,
      MetaSource.TableMetaModel tableMetaModel) {
    String ossFileName = OssUtils.getOssPathToExportObject(
        restoreConfig.getBackupName(),
        Constants.EXPORT_TABLE_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        Constants.EXPORT_PARTITION_SPEC_FILE_NAME);
    if (OssUtils.exists(restoreConfig.getOssConfig(), ossFileName)) {
      return generatePartitionedTableRestoreTask(
          jobId,
          taskIdPrefix,
          restoreConfig,
          tableMetaModel);
    } else {
      LOG.info("Restore partition info file not found for {}", restoreConfig);
      return generateNonPartitionedTableRestoreTask(
          jobId,
          taskIdPrefix,
          restoreConfig,
          tableMetaModel);
    }
  }

  private Task generatePartitionedTableRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectRestoreConfig restoreConfig,
      TableMetaModel tableMetaModel) {
    Task task = null;
    String temporaryTableName = generateRestoredTemporaryTableName(restoreConfig);

    try {
      DropRestoredTemporaryTableWorkItem dropRestoredTemporaryTableWorkItem =
          new DropRestoredTemporaryTableWorkItem(
              jobId,
              taskIdPrefix + ".DropRestoredTemporaryTable",
              restoreConfig.getDestinationDatabaseName().toLowerCase(),
              temporaryTableName.toLowerCase(),
              restoreConfig.getOdpsConfig(),
              restoreConfig.getOssConfig(),
              tableMetaModel,
              mmaMetaManager,
              this);
      boolean jobExists = mmaMetaManager.hasMigrationJob(
          jobId,
          MmaConfig.JobType.RESTORE.name(),
          restoreConfig.getObjectType().name(),
          restoreConfig.getDestinationDatabaseName(),
          temporaryTableName);
      if (!jobExists) {
        DirectedAcyclicGraph<Action, DefaultEdge> dag =
            new DirectedAcyclicGraph<>(DefaultEdge.class);
        String backupName = restoreConfig.getBackupName();
        OdpsRestoreTableAction restoreTableAction = new OdpsRestoreTableAction(
            taskIdPrefix + ".RestoreTable",
            backupName,
            restoreConfig.getSourceDatabaseName(),
            restoreConfig.getObjectName(),
            restoreConfig.getDestinationDatabaseName(),
            temporaryTableName,
            restoreConfig.getObjectType(),
            restoreConfig.getSettings());
        dag.addVertex(restoreTableAction);
        if (restoreConfig.isUpdate()) {
          OdpsDropTableAction dropTableAction = new OdpsDropTableAction(
              taskIdPrefix + ".DropTableToBeRestored", false);
          dag.addVertex(dropTableAction);
          dag.addEdge(dropTableAction, restoreTableAction);
        }

        OdpsRestorePartitionAction restorePartitionAction = new OdpsRestorePartitionAction(
            taskIdPrefix + ".RestorePartition",
            backupName,
            restoreConfig.getSourceDatabaseName(),
            restoreConfig.getObjectName(),
            restoreConfig.getDestinationDatabaseName(),
            temporaryTableName,
            restoreConfig.getSettings());

        MmaConfig.TableMigrationConfig config = new MmaConfig.TableMigrationConfig(
            MmaServerConfig.getInstance().getDataSource(),
            restoreConfig.getDestinationDatabaseName(),
            temporaryTableName,
            restoreConfig.getDestinationDatabaseName(),
            restoreConfig.getObjectName(),
            MmaConfigUtils.DEFAULT_ADDITIONAL_TABLE_CONFIG);
        config.setOdpsConfig(restoreConfig.getOdpsConfig());

        AddMigrationJobAction addMigrationJobAction = new AddMigrationJobAction(
            taskIdPrefix + ".AddMigrationJobAction",
            config,
            mmaMetaManager);
        AddBackgroundWorkItemAction addBackgroundWorkItemAction = new AddBackgroundWorkItemAction(
            taskIdPrefix + ".AddBackgroundWorkItem",
            backgroundLoopManager,
            dropRestoredTemporaryTableWorkItem);
        dag.addVertex(restorePartitionAction);
        dag.addVertex(addMigrationJobAction);
        dag.addVertex(addBackgroundWorkItemAction);

        dag.addEdge(restoreTableAction, restorePartitionAction);
        dag.addEdge(restorePartitionAction, addMigrationJobAction);
        dag.addEdge(addMigrationJobAction, addBackgroundWorkItemAction);

        task = new OdpsRestoreTablePrepareTask(
            taskIdPrefix,
            jobId,
            tableMetaModel,
            dag,
            mmaMetaManager);
        ((OdpsRestoreTablePrepareTask)task).setOdpsConfig(restoreConfig.getOdpsConfig());
        ((OdpsRestoreTablePrepareTask)task).setOssConfig(restoreConfig.getOssConfig());
      } else {
        LOG.info("Migration temporary table job already exist for {}",
                 MmaConfig.ObjectRestoreConfig.toJson(restoreConfig));
        backgroundLoopManager.addWorkItem(dropRestoredTemporaryTableWorkItem);
      }
    } catch (Exception e) {
      LOG.error("Exception when generate partitioned table restore task {}",
                MmaConfig.ObjectRestoreConfig.toJson(restoreConfig), e);
    }
    return task;
  }

  private String generateRestoredTemporaryTableName(MmaConfig.ObjectRestoreConfig restoreConfig) {
    return "_temporary_table_generated_by_mma_" + restoreConfig.getObjectName() + "_in_restore_task_" + restoreConfig
        .getBackupName();
  }

  private Task generateNonPartitionedTableRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectRestoreConfig restoreConfig,
      TableMetaModel tableMetaModel) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    String backupName = restoreConfig.getBackupName();
    MmaConfig.ObjectType objectType = restoreConfig.getObjectType();
    String temporaryTableName = generateRestoredTemporaryTableName(restoreConfig);
    OdpsRestoreTableAction restoreTableAction = new OdpsRestoreTableAction(
        taskIdPrefix + ".Restore" + objectType.name(),
        backupName,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        tableMetaModel.odpsProjectName,
        temporaryTableName,
        objectType,
        restoreConfig.getSettings());
    dag.addVertex(restoreTableAction);
    if (restoreConfig.isUpdate()) {
      OdpsDropTableAction dropTableAction = new OdpsDropTableAction(
          taskIdPrefix + ".Drop" + objectType,
          MmaConfig.ObjectType.VIEW.equals(objectType));
      dag.addVertex(dropTableAction);
      dag.addEdge(dropTableAction, restoreTableAction);
    }
    OdpsResetTableMetaModelAction resetTableMetaModelAction = new OdpsResetTableMetaModelAction(
        taskIdPrefix + ".ResetTableMetaModel",
        tableMetaModel.odpsProjectName,
        temporaryTableName,
        restoreConfig);
    dag.addVertex(resetTableMetaModelAction);
    dag.addEdge(restoreTableAction, resetTableMetaModelAction);
    OdpsCreateTableAction createTableAction =
        new OdpsCreateTableAction(taskIdPrefix + ".CreateTable");

    OdpsInsertOverwriteDataTransferAction dataTransferAction =
        new OdpsInsertOverwriteDataTransferAction(taskIdPrefix + ".DataTransfer");

    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskIdPrefix + ".DestVerification");

    OdpsSourceVerificationAction sourceVerificationAction =
        new OdpsSourceVerificationAction(taskIdPrefix + ".SourceVerification");

    VerificationAction verificationAction = new VerificationAction(taskIdPrefix + ".Compare");
    OdpsDropTableAction dropTemporaryTableAction = new OdpsDropTableAction(
        taskIdPrefix + ".DropTemporaryTable",
        tableMetaModel.odpsProjectName,
        temporaryTableName,
        false);
    dag.addVertex(createTableAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);
    dag.addVertex(dropTemporaryTableAction);

    dag.addEdge(resetTableMetaModelAction, createTableAction);
    dag.addEdge(createTableAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);
    dag.addEdge(verificationAction, dropTemporaryTableAction);
    return new ObjectRestoreTask(
        taskIdPrefix,
        jobId,
        restoreConfig.getObjectType().name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private Task generateViewRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectRestoreConfig restoreConfig,
      TableMetaModel tableMetaModel) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsRestoreTableAction restoreTableAction = new OdpsRestoreTableAction(
        taskIdPrefix + ".RestoreView",
        restoreConfig.getBackupName(),
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        tableMetaModel.odpsProjectName,
        tableMetaModel.tableName,
        restoreConfig.getObjectType(),
        restoreConfig.getSettings());
    dag.addVertex(restoreTableAction);
    if (restoreConfig.isUpdate()) {
      OdpsDropTableAction dropTableAction =
          new OdpsDropTableAction(taskIdPrefix + ".DropView", true);
      dag.addVertex(dropTableAction);
      dag.addEdge(dropTableAction, restoreTableAction);
    }
    return new ObjectRestoreTask(
        taskIdPrefix,
        jobId,
        restoreConfig.getObjectType().name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private Task generateDatabaseRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.DatabaseRestoreConfig restoreConfig,
      TableMetaModel tableMetaModel) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsDatabaseRestoreAction tableRestoreAction = new OdpsDatabaseRestoreAction(
        taskIdPrefix + ".Table",
        MmaConfig.ObjectType.TABLE,
        restoreConfig,
        mmaMetaManager);
    OdpsDatabaseRestoreAction viewRestoreAction = new OdpsDatabaseRestoreAction(
        taskIdPrefix + ".View",
        MmaConfig.ObjectType.VIEW,
        restoreConfig,
        mmaMetaManager);
    OdpsDatabaseRestoreAction resourceRestoreAction = new OdpsDatabaseRestoreAction(
        taskIdPrefix + ".Resource",
        MmaConfig.ObjectType.RESOURCE,
        restoreConfig,
        mmaMetaManager);
    OdpsDatabaseRestoreAction functionRestoreAction = new OdpsDatabaseRestoreAction(
        taskIdPrefix + ".Function",
        MmaConfig.ObjectType.FUNCTION,
        restoreConfig,
        mmaMetaManager);

    dag.addVertex(tableRestoreAction);
    dag.addVertex(viewRestoreAction);
    dag.addVertex(resourceRestoreAction);
    dag.addVertex(functionRestoreAction);

    dag.addEdge(tableRestoreAction, viewRestoreAction);
    dag.addEdge(viewRestoreAction, resourceRestoreAction);
    dag.addEdge(resourceRestoreAction, functionRestoreAction);
    return new ObjectRestoreTask(
        taskIdPrefix,
        jobId,
        MmaConfig.ObjectType.DATABASE.name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private Task generateFunctionRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectRestoreConfig restoreConfig,
      TableMetaModel tableMetaModel) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsRestoreFunctionAction action =
        new OdpsRestoreFunctionAction(taskIdPrefix + ".RestoreFunction", restoreConfig);
    dag.addVertex(action);
    return new ObjectRestoreTask(
        taskIdPrefix,
        jobId,
        restoreConfig.getObjectType().name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private Task generateResourceRestoreTask(
      String jobId,
      String taskIdPrefix,
      MmaConfig.ObjectRestoreConfig restoreConfig,
      TableMetaModel tableMetaModel) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsRestoreResourceAction action =
        new OdpsRestoreResourceAction(taskIdPrefix + ".RestoreFunction", restoreConfig);
    dag.addVertex(action);
    return new ObjectRestoreTask(
        taskIdPrefix,
        jobId,
        restoreConfig.getObjectType().name(),
        tableMetaModel,
        dag,
        mmaMetaManager);
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getHiveNonPartitionedTableMigrationActionDag(
      String taskId) {
    OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropTable");
    OdpsCreateTableAction createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    HiveUdtfDataTransferAction dataTransferAction =
        new HiveUdtfDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    HiveSourceVerificationAction sourceVerificationAction =
        new HiveSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    dag.addVertex(dropTableAction);
    dag.addVertex(createTableAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(dropTableAction, createTableAction);
    dag.addEdge(createTableAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getHivePartitionedTableMigrationActionDag(String taskId) {
    OdpsCreateTableAction createTableAction =
        new OdpsCreateTableAction(taskId + ".CreateTable");
    OdpsDropPartitionAction dropPartitionAction =
        new OdpsDropPartitionAction(taskId + ".DropPartition");
    OdpsAddPartitionAction addPartitionAction =
        new OdpsAddPartitionAction(taskId + ".AddPartition");
    HiveUdtfDataTransferAction dataTransferAction =
        new HiveUdtfDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    HiveSourceVerificationAction sourceVerificationAction =
        new HiveSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    dag.addVertex(createTableAction);
    dag.addVertex(dropPartitionAction);
    dag.addVertex(addPartitionAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(createTableAction, dropPartitionAction);
    dag.addEdge(dropPartitionAction, addPartitionAction);
    dag.addEdge(addPartitionAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsNonPartitionedTableMigrationActionDag(
      String taskId) {
    return getOdpsNonPartitionedTableMigrationActionDag(
        taskId, false, null);
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsNonPartitionedTableMigrationActionDag(
      String taskId, boolean toExternalTable, ExternalTableConfig externalTableConfig) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

    OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropTable");

    Action createTableAction;
    if (toExternalTable) {
      switch (externalTableConfig.getStorage()) {
        case OSS:
          createTableAction =
              new OdpsCreateOssExternalTableAction(taskId + ".CreateExternalTable", externalTableConfig.getLocation());
          break;
        default:
          throw new IllegalArgumentException("Unsupported external table storage");
      }
    } else {
      createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    }

    OdpsInsertOverwriteDataTransferAction dataTransferAction =
        new OdpsInsertOverwriteDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    OdpsSourceVerificationAction sourceVerificationAction =
        new OdpsSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    dag.addVertex(dropTableAction);
    dag.addVertex(createTableAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(dropTableAction, createTableAction);
    dag.addEdge(createTableAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsPartitionedTableMigrationActionDag(String taskId) {
    return getOdpsPartitionedTableMigrationActionDag(taskId, false, null);
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsPartitionedTableMigrationActionDag(String taskId, boolean toExternalTable, ExternalTableConfig externalTableConfig) {

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

    Action createTableAction;
    if (toExternalTable) {
      switch (externalTableConfig.getStorage()) {
        case OSS:
          createTableAction = new OdpsCreateOssExternalTableAction(
              taskId + ".CreateExternalTable", externalTableConfig.getLocation());
          break;
        default:
          throw new IllegalArgumentException("Unsupported external table storage");
      }
    } else {
      createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    }

    Action addPartitionAction = new OdpsAddPartitionAction(taskId + ".AddPartition");
    OdpsInsertOverwriteDataTransferAction dataTransferAction =
        new OdpsInsertOverwriteDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    OdpsSourceVerificationAction sourceVerificationAction =
        new OdpsSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    dag.addVertex(createTableAction);
    dag.addVertex(addPartitionAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(createTableAction, addPartitionAction);
    dag.addEdge(addPartitionAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private List<TableMetaModel> getTableSplits(
      TableMetaModel tableMetaModel,
      MmaConfig.AdditionalTableConfig config) {
    if (tableMetaModel.partitionColumns.isEmpty() || tableMetaModel.partitions.size() == 0) {
      // Splitting non-partitioned tables not supported
      return Collections.singletonList(tableMetaModel);
    }

    List<TableMetaModel> splits = getAdaptiveTableSplits(tableMetaModel, config);
    if (splits != null) {
      return splits;
    }

    return getStaticTableSplits(tableMetaModel, config);
  }

  static List<TableMetaModel> getStaticTableSplits(
      TableMetaModel tableMetaModel,
      MmaConfig.AdditionalTableConfig config) {
    LOG.info("Database: {}, table: {}, enter getStaticTableSplits",
             tableMetaModel.databaseName, tableMetaModel.tableName);
    List<TableMetaModel> ret = new LinkedList<>();
    int partitionGroupSize;
    if (config != null && config.getPartitionGroupSize() > 0) {
      partitionGroupSize = config.getPartitionGroupSize();
    } else {
      partitionGroupSize =
          Math.min(tableMetaModel.partitions.size(), Constants.MAX_PARTITION_GROUP_SIZE);
    }

    int startIdx = 0;
    while (startIdx < tableMetaModel.partitions.size()) {
      TableMetaModel clone = tableMetaModel.clone();

      // Set partitions
      int endIdx = Math.min(tableMetaModel.partitions.size(), startIdx + partitionGroupSize);
      clone.partitions = new ArrayList<>(tableMetaModel.partitions.subList(startIdx, endIdx));
      ret.add(clone);

      startIdx += partitionGroupSize;
    }

    return ret;
  }

  static List<TableMetaModel> getAdaptiveTableSplits(
      TableMetaModel tableMetaModel,
      MmaConfig.AdditionalTableConfig config) {
    LOG.info("Database: {}, table: {}, enter getAdaptiveTableSplits",
             tableMetaModel.databaseName,
             tableMetaModel.tableName);
    if (tableMetaModel.partitionColumns.isEmpty()) {
      LOG.info("Database: {}, table: {}, adaptive table splitting not working with non-partitioned table",
               tableMetaModel.databaseName, tableMetaModel.tableName);
      return null;
    }

    List<MetaSource.PartitionMetaModel> partitionsWithoutSize = tableMetaModel.partitions
        .stream().filter(p -> (p.size == null)).collect(Collectors.toList());
    if (!partitionsWithoutSize.isEmpty()) {
      partitionsWithoutSize.forEach(
          p -> LOG.info("Database: {}, table: {}, partition: {}, size not available",
                        tableMetaModel.databaseName, tableMetaModel.tableName, p.partitionValues));
      return null;
    }

    List<TableMetaModel> ret = new LinkedList<>();
    final long splitSizeInByte = (config.getPartitionGroupSplitSizeInGb() * 1024 * 1024) * 1024L;

    // Sort in descending order
    tableMetaModel.partitions.sort((o1, o2) -> {
      if (o1.size > o2.size) {
        return -1;
      } else if (o1.size.equals(o2.size)) {
        return 0;
      } else {
        return 1;
      }
    });

    int i = 0;
    while (i < tableMetaModel.partitions.size()) {
      TableMetaModel clone = tableMetaModel.clone();

      // Handle extremely large partitions. Generate a task for each of them.
      if (tableMetaModel.partitions.get(i).size > splitSizeInByte) {
        LOG.info("Database: {}, table: {}, partition: {}, size exceeds split size: {}",
                 tableMetaModel.databaseName,
                 tableMetaModel.tableName,
                 tableMetaModel.partitions.get(i).partitionValues,
                 tableMetaModel.partitions.get(i).size);
        clone.partitions = Collections.singletonList(tableMetaModel.partitions.get(i));
        i++;
        ret.add(clone);
        continue;
      }

      clone.partitions = new LinkedList<>();
      LOG.debug("Database: {}, table: {}, assemble {} th partition group",
                tableMetaModel.databaseName, tableMetaModel.tableName, ret.size());
      long sum = 0L;

      // Keep adding partitions as long as the total size is less than splitSizeInByte and the
      // number of partitions is less than Constants.MAX_PARTITION_GROUP_SIZE
      while (i < tableMetaModel.partitions.size()
          && clone.partitions.size() < Constants.MAX_PARTITION_GROUP_SIZE) {
        if (sum + tableMetaModel.partitions.get(i).size <= splitSizeInByte) {
          clone.partitions.add(tableMetaModel.partitions.get(i));
          LOG.debug(
              "Database: {}, table: {}, add partition: {}, size: {} to partition group",
              tableMetaModel.databaseName,
              tableMetaModel.tableName,
              tableMetaModel.partitions.get(i).partitionValues,
              tableMetaModel.partitions.get(i).size);
          sum += tableMetaModel.partitions.get(i).size;
        } else {
          break;
        }
        i++;
      }

      ret.add(clone);
      LOG.debug(
          "Database: {}, table: {}, {} th partition group, num partitions: {}, size: {}",
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          ret.size(),
          ((LinkedList<TableMetaModel>) ret).getLast().partitions.size(),
          ((LinkedList<TableMetaModel>) ret).getLast().partitions.stream().map(p -> p.size).reduce((s1, s2) -> s1 + s2));
    }

    return ret;
  }

  private String getTaskIdPrefix(
      MmaConfig.JobType jobType,
      MmaConfig.ObjectType objectType,
      String db, String object) {
    if (MmaConfig.ObjectType.DATABASE.equals(objectType)) {
      return String.format("%s-%s-%s", jobType.name(), objectType.name(), db);
    }
    return String.format("%s-%s-%s-%s", jobType.name(), objectType.name(), db, object);
  }
}
