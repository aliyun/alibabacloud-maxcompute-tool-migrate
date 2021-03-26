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

package com.aliyun.odps.datacarrier.taskscheduler.meta;

import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.createMmaPartitionMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.createMmaRestoreTable;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.createMmaTableMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.createMmaTemporaryTable;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.deleteFromMmaMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.dropMmaPartitionMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.getPartitionStatusDistribution;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.inferPartitionedTableStatus;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.mergeIntoMmaPartitionMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.mergeIntoMmaTableMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.mergeIntoRestoreTableMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.removeActiveTasksFromRestoreTable;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.selectFromMmaTableMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.selectFromRestoreMeta;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.selectFromTemporaryTableMeta;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
import com.aliyun.odps.datacarrier.taskscheduler.ExternalTableStorage;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.DatabaseRestoreConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.ObjectBackupConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.ObjectRestoreConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.ObjectType;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfigUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaExceptionFactory;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.JobInfo;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.MigrationJobPtInfo;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.RestoreJobInfo;
import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MmaMetaManagerDbImpl implements MmaMetaManager {
  private static final Logger LOG = LogManager.getLogger(MmaMetaManagerDbImpl.class);

  private HikariDataSource ds;
  private MmaConfig.MetaDbConfig metaDbConfig;

  public MmaMetaManagerDbImpl(boolean needRecover) throws MmaException {
    LOG.info("Initialize MmaMetaManagerDbImpl");
    metaDbConfig = MmaServerConfig.getInstance().getMetaDbConfig();
    try {
      Class.forName(metaDbConfig.getDriverClass());
    } catch (ClassNotFoundException e) {
      LOG.error("JDBC driver {} not found", metaDbConfig.getDriverClass());
      throw new IllegalStateException("Class not found: " + metaDbConfig.getDriverClass());
    }

    LOG.info("Create connection pool");
    setupDatasource();
    LOG.info("JDBC connection URL: {}", metaDbConfig.getJdbcUrl());

    LOG.info("Create connection pool done");

    LOG.info("Setup database");
    try (Connection conn = ds.getConnection()) {
      createMmaTableMeta(conn);
      createMmaRestoreTable(conn);
      removeActiveTasksFromRestoreTable(conn);
      createMmaTemporaryTable(conn);
      conn.commit();
    } catch (Throwable e) {
      LOG.error("Setup database failed", e);
      throw new MmaException("Setting up database failed", e);
    }
    LOG.info("Setup database done");

    if (needRecover) {
      try {
        recover();
      } catch (Throwable e) {
        throw new IllegalStateException("Recover failed", e);
      }
    }
    LOG.info("Initialize MmaMetaManagerDbImpl done");
  }

  private void setupDatasource() {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(metaDbConfig.getJdbcUrl());
    hikariConfig.setUsername(metaDbConfig.getUser());
    hikariConfig.setPassword(metaDbConfig.getPassword());
    hikariConfig.setAutoCommit(false);
    hikariConfig.setMaximumPoolSize(metaDbConfig.getMaxPoolSize());
    hikariConfig.setMinimumIdle(1);
    hikariConfig.setTransactionIsolation("TRANSACTION_SERIALIZABLE");
    ds = new HikariDataSource(hikariConfig);
  }

  @Override
  public void shutdown() {
    LOG.info("Enter shutdown");
    ds.close();
    LOG.info("Leave shutdown");
  }

  private synchronized void recover() throws SQLException, MmaException {
    LOG.info("Enter recover");

    try (Connection conn = ds.getConnection()) {
     List<JobInfo> jobInfos = selectFromMmaTableMeta(conn, null, -1);
      for (JobInfo jobInfo : jobInfos) {
        if (MmaMetaManager.JobStatus.RUNNING.equals(jobInfo.getStatus())) {
          updateStatusInternal(
              conn,
              jobInfo.getJobId(),
              jobInfo.getJobType().name(),
              jobInfo.getObjectType().name(),
              jobInfo.getDb(),
              jobInfo.getObject(),
              MmaMetaManager.JobStatus.PENDING);
        }

        if (jobInfo.isPartitioned()) {
          List<MigrationJobPtInfo> jobPtInfos =
              selectFromMmaPartitionMeta(
                  conn,
                  jobInfo.getJobId(),
                  jobInfo.getJobType().name(),
                  jobInfo.getDb(),
                  jobInfo.getObject(),
                  MmaMetaManager.JobStatus.RUNNING,
                  -1);

          updateStatusInternal(
              conn,
              jobInfo.getJobId(),
              jobInfo.getJobType().name(),
              jobInfo.getObjectType().name(),
              jobInfo.getDb(),
              jobInfo.getObject(),
              jobPtInfos
                  .stream()
                  .map(MigrationJobPtInfo::getPartitionValues)
                  .collect(Collectors.toList()),
              MmaMetaManager.JobStatus.PENDING);
        }
      }

      conn.commit();
    }
    LOG.info("Leave recover");
  }

  @Override
  public synchronized void addMigrationJob(
      DataSource dataSource,
      String jobId,
      MmaConfig.TableMigrationConfig config) throws MmaException {
    MetaSource metaSource;
    LOG.info("Enter addMigrationJob");
    if (config == null) {
      throw new IllegalArgumentException("'config' cannot be null");
    }
    String db = config.getSourceDataBaseName().toLowerCase();
    String tbl = config.getSourceTableName().toLowerCase();
    String destDb = config.getDestProjectName().toLowerCase();
    String destTbl = config.getDestTableName().toLowerCase();
    LOG.info("Add migration job, db: {}, tbl: {} to db: {}, tbl: {}",
             db, tbl, destDb, destTbl);

    switch (dataSource) {
      case ODPS:
        metaSource = MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());
        break;
      case Hive:
        try {
          metaSource = MetaSourceFactory.getHiveMetaSource(
              MmaServerConfig.getInstance().getHiveConfig(),
              MmaServerConfig.getInstance().getHdfsConfig());
        } catch (MetaException e) {
          throw new MmaException("Error initializing meta source", e);
        }
        break;

      default:
        throw new MmaException("Unsupported data source");
    }

    mergeJobInfoIntoMetaDB(
        jobId,
        db,
        tbl,
        true,
        MmaConfig.JobType.MIGRATION,
        MmaConfig.ObjectType.TABLE,
        MmaConfig.TableMigrationConfig.toJson(config),
        config.getAdditionalTableConfig(),
        config.getPartitionValuesList(),
        config.getBeginPartition(),
        config.getEndPartition(),
        metaSource);
  }

  @Override
  public synchronized void addObjectBackupJob(
      String jobId, MmaConfig.ObjectBackupConfig config) throws MmaException {
    String db = config.getDatabaseName().toLowerCase();
    String object = config.getObjectName().toLowerCase();

    LOG.info("Add backup job, db: {}, object: {}, type: {}",
             db, object, config.getObjectType().name());

    MetaSource metaSource = MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());
    mergeJobInfoIntoMetaDB(
        jobId,
        db,
        object,
        MmaConfig.ObjectType.TABLE.equals(config.getObjectType()),
        MmaConfig.JobType.BACKUP,
        config.getObjectType(),
        MmaConfig.ObjectBackupConfig.toJson(config),
        MmaConfigUtils.DEFAULT_ADDITIONAL_TABLE_CONFIG,
        null,
        null,
        null,
        metaSource);
  }

  @Override
  public synchronized void addObjectRestoreJob(
      String jobId, MmaConfig.ObjectRestoreConfig config) throws MmaException {
    String db = config.getSourceDatabaseName().toLowerCase();
    String object = config.getObjectName().toLowerCase();
    LOG.info("Add restore job, from {} to {}, object: {}, type: {}",
             config.getSourceDatabaseName(),
             config.getDestinationDatabaseName(),
             object,
             config.getObjectType().name());

    MetaSource metaSource = MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());

    mergeJobInfoIntoMetaDB(
        jobId,
        db,
        object,
        false,
        MmaConfig.JobType.RESTORE,
        config.getObjectType(),
        MmaConfig.ObjectRestoreConfig.toJson(config),
        MmaConfigUtils.DEFAULT_ADDITIONAL_TABLE_CONFIG,
        null,
        null,
        null,
        metaSource);
  }

  @Override
  public synchronized void addDatabaseRestoreJob(
      String jobId, MmaConfig.DatabaseRestoreConfig config) throws MmaException {
    String db = config.getSourceDatabaseName().toLowerCase();
    LOG.info("Add restore database job, from {} to {}, types: {}",
             config.getSourceDatabaseName(),
             config.getDestinationDatabaseName(),
             config.getObjectTypes());

    MetaSource metaSource = MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());

    mergeJobInfoIntoMetaDB(
        jobId,
        db,
        "",
        false,
        MmaConfig.JobType.RESTORE,
        MmaConfig.ObjectType.DATABASE,
        MmaConfig.DatabaseRestoreConfig.toJson(config),
        MmaConfigUtils.DEFAULT_ADDITIONAL_TABLE_CONFIG,
        null,
        null,
        null,
        metaSource);
  }

  @Override
  public synchronized void mergeJobInfoIntoRestoreDB(
      RestoreJobInfo jobInfo) throws MmaException {
    try (Connection conn = ds.getConnection()) {
      try {
        mergeIntoRestoreTableMeta(conn, jobInfo);
        conn.commit();
      } catch (Throwable e) {
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Add restore job rollback failed, job info {}", GsonUtils.getFullConfigGson().toJson(jobInfo));
          }
        }
        LOG.error(e);
        throw new MmaException("Merge job info to restore db fail: " + GsonUtils.getFullConfigGson().toJson(jobInfo), e);
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized void updateStatusInRestoreDB(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String object,
      MmaMetaManager.JobStatus newStatus) throws MmaException {
    int retryTimesLimit, attemptTimes;
    String conditionBuilder = "WHERE "
        + String.format("%s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID, jobId)
        + String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_OBJECT_TYPE, objectType)
        + String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_DB_NAME, db)
        + String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME, object);
    List<RestoreJobInfo> currentInfos = listRestoreJobs(conditionBuilder, -1);

    if (currentInfos.isEmpty()) {
      String errMsg = "Restore object job not found,"
          + " jobId: " + jobId
          + ", obj type: " + objectType
          + ", db: " + db
          + ", object: " + object;
      throw new MmaException(errMsg);
    }

    RestoreJobInfo currentInfo = currentInfos.get(0);
    switch (newStatus) {
      case SUCCEEDED:
        currentInfo.setStatus(newStatus);
        currentInfo.setAttemptTimes(currentInfo.getAttemptTimes() + 1);
        break;
      case FAILED:
        attemptTimes = currentInfo.getAttemptTimes() + 1;
        retryTimesLimit = currentInfo
            .getJobConfig()
            .getAdditionalTableConfig()
            .getRetryTimesLimit();
        if (attemptTimes <= retryTimesLimit) {
          newStatus = MmaMetaManager.JobStatus.PENDING;
        }
        currentInfo.setStatus(newStatus);
        currentInfo.setAttemptTimes(attemptTimes);
        break;
    }
    mergeJobInfoIntoRestoreDB(currentInfo);
  }

  private void mergeJobInfoIntoMetaDB(
      String uniqueId,
      String db,
      String object,
      boolean isTable,
      MmaConfig.JobType jobType,
      MmaConfig.ObjectType objectType,
      String config,
      MmaConfig.AdditionalTableConfig additionalTableConfig,
      List<List<String>> partitionValuesList,
      List<String> beginPartition,
      List<String> endPartition,
      MetaSource metaSource) throws MmaException {
    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = selectFromMmaTableMeta(
            conn, uniqueId, jobType.name(), objectType.name(), db, object);

        if (jobInfo != null && MmaMetaManager.JobStatus.RUNNING.equals(jobInfo.getStatus())) {
          throw MmaExceptionFactory.getRunningJobExistsException(db, object);
        }

        boolean isPartitioned = false;
        if (isTable) {
          MetaSource.TableMetaModel tableMetaModel =
              metaSource.getTableMetaWithoutPartitionMeta(db, object);
          isPartitioned = tableMetaModel.partitionColumns.size() > 0;

          // Create or update mma partition meta
          // If partitions are specified, MMA will only create or update these partition. Else, MMA
          // will fetch all the partitions, then create meta for new partitions, reset meta for
          // failed partitions and modified succeeded partitions.
          // TODO: this behavior should be configurable
          if (isPartitioned) {
            createMmaPartitionMetaSchema(conn, db);
            createMmaPartitionMeta(conn, db, object);

            List<MigrationJobPtInfo> jobPtInfosToMerge = new LinkedList<>();

            if (partitionValuesList != null) {
              for (List<String> partitionValues : partitionValuesList) {
                if (!metaSource.hasPartition(db, object, partitionValues)) {
                  throw new MmaException("Partition not found: " + partitionValues);
                }

                MetaSource.PartitionMetaModel partitionMetaModel =
                    metaSource.getPartitionMeta(db, object, partitionValues);
                jobPtInfosToMerge.add(
                    new MigrationJobPtInfo(
                        partitionValues,
                        MmaMetaManager.JobStatus.PENDING,
                        Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                        partitionMetaModel.lastModifiedTime));
              }
            } else {
              List<MigrationJobPtInfo> jobPtInfos = selectFromMmaPartitionMeta(
                  conn, uniqueId, jobType.name(), db, object, null, -1);

              Comparator<List<String>> partitionComparator = (o1, o2) -> {
                int ret = 0;
                // o1.size() != o2.size() is allowed. Implicit partition range depends on this.
                for (int i = 0; i < o1.size(); i++) {
                  if (o1.get(i).length() < o2.get(i).length()) {
                    ret = -1;
                  } else if (o1.get(i).length() > o2.get(i).length()) {
                    ret = 1;
                  } else {
                    ret = o1.get(i).compareTo(o2.get(i));
                  }
                  if (ret != 0 || i == o2.size() - 1) {
                    break;
                  }
                }
                return ret;
              };

              List<List<String>> totalPartitionValuesList = metaSource.listPartitions(db, object);

              if (beginPartition != null
                  && endPartition != null
                  && partitionComparator.compare(beginPartition, endPartition) > 0) {
                throw new IllegalArgumentException("Invalid begin and end partition, begin partition > end partition");
              }

              if (beginPartition != null) {
                if (beginPartition.size() > tableMetaModel.partitionColumns.size()) {
                  throw new IllegalArgumentException(
                      "Invalid begin partition, number of elements > number of partition columns");
                }
                totalPartitionValuesList = totalPartitionValuesList
                    .stream()
                    .filter(list -> (partitionComparator.compare(beginPartition, list) <= 0))
                    .collect(Collectors.toList());
              }

              if (endPartition != null) {
                if (endPartition.size() > tableMetaModel.partitionColumns.size()) {
                  throw new IllegalArgumentException(
                      "Invalid end partition, number of elements > number of partition columns");
                }
                totalPartitionValuesList = totalPartitionValuesList
                    .stream()
                    .filter(list -> (partitionComparator.compare(endPartition, list) >= 0))
                    .collect(Collectors.toList());
              }

              // Iterate over latest partition list and try to find partitions that should be
              // migrated
              for (List<String> partitionValues : totalPartitionValuesList) {
                MmaMetaManagerDbImplUtils.MigrationJobPtInfo jobPtInfo = jobPtInfos
                    .stream()
                    .filter(info -> info.getPartitionValues().equals(partitionValues))
                    .findAny()
                    .orElse(null);

                MetaSource.PartitionMetaModel partitionMetaModel =
                    metaSource.getPartitionMeta(db, object, partitionValues);

                if (jobPtInfo == null
                    || MmaMetaManager.JobStatus.FAILED.equals(jobPtInfo.getStatus())) {
                  if (jobPtInfo == null) {
                    LOG.info("Found new partition: {}", partitionValues);
                  } else {
                    LOG.info("Found failed partition: {}", partitionValues);
                  }
                  // New partition or failed partition
                  jobPtInfosToMerge.add(new MigrationJobPtInfo(
                      partitionValues,
                      MmaMetaManager.JobStatus.PENDING,
                      Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                      partitionMetaModel.lastModifiedTime));
                } else if (MmaMetaManager.JobStatus.SUCCEEDED.equals(jobPtInfo.getStatus())) {
                  // Modified partitions
                  if (partitionMetaModel.lastModifiedTime == null) {
                    LOG.warn("Failed to get last modified time of partition {}",
                             partitionValues);
                  } else if (partitionMetaModel.lastModifiedTime > jobPtInfo.getLastModifiedTime()) {
                    LOG.info("Found modified partition, {}, old mtime: {}, new mtime: {}",
                             partitionValues,
                             jobPtInfo.getLastModifiedTime(),
                             partitionMetaModel.lastModifiedTime);

                    jobPtInfosToMerge.add(new MigrationJobPtInfo(
                        partitionValues,
                        MmaMetaManager.JobStatus.PENDING,
                        Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                        partitionMetaModel.lastModifiedTime));
                  }
                }
              }
            }

            mergeIntoMmaPartitionMeta(
                conn, uniqueId, jobType.name(), db, object, jobPtInfosToMerge);
          }
        }
        mergeObjectInfoIntoMetaDB(
            uniqueId,
            db,
            object,
            jobType,
            objectType,
            config,
            additionalTableConfig,
            isPartitioned,
            conn);

        conn.commit();
        LOG.info("Leave addMigrationJob");
      } catch (Throwable e) {
        // Rollback
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Add migration job rollback failed, db: {}, object: {}", db, object);
          }
        }

        MmaException mmaException =
            MmaExceptionFactory.getFailedToAddMigrationJobException(db, object, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  private void mergeObjectInfoIntoMetaDB(
      String uniqueId,
      String db,
      String object,
      MmaConfig.JobType jobType,
      MmaConfig.ObjectType objectType,
      String jobDescription,
      MmaConfig.AdditionalTableConfig additionalTableConfig,
      boolean isPartitioned,
      Connection conn) throws SQLException {
    MmaConfig.JobConfig jobConfig = new MmaConfig.JobConfig(jobDescription, additionalTableConfig);
    JobInfo jobInfo = new JobInfo(
        uniqueId,
        jobType.name(),
        objectType.name(),
        db,
        object,
        isPartitioned,
        jobConfig,
        MmaMetaManager.JobStatus.PENDING,
        Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
        Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);

    mergeIntoMmaTableMeta(conn, jobInfo);
  }

  @Override
  public synchronized void removeMigrationJob(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String object) throws MmaException {
    LOG.info("Enter removeMigrationJob, uniqueId: {}, jobType: {}, objectType: {}, db: {}, object: {}",
             jobId, jobType, objectType, db, object);

    if (db == null || object == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    object = object.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = selectFromMmaTableMeta(conn, jobId, jobType, objectType, db, object);
        if (jobInfo == null) {
          return;
        } else if (MmaMetaManager.JobStatus.RUNNING
            .equals(getStatusInternal(jobId, jobType, objectType, db, object))) {
          MmaException e = MmaExceptionFactory.getRunningJobExistsException(db, object);
          LOG.error(e);
          throw e;
        }

        if (jobInfo.isPartitioned()) {
          dropMmaPartitionMeta(conn, db, object);
        }
        deleteFromMmaMeta(conn, jobId, jobType, objectType, db, object);

        conn.commit();
        LOG.info("Leave removeMigrationJob");
      } catch (Throwable e) {
        // Rollback
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Remove migration job rollback failed, db: {}, tbl: {}", db, object);
          }
        }
        MmaException mmaException =
            MmaExceptionFactory.getFailedToRemoveMigrationJobException(db, object, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized void removeJob(String jobId) throws MmaException {
    LOG.info("Enter removeJob, job id: {}", jobId);

    try (Connection conn = ds.getConnection()) {
      try {
        deleteFromMmaMeta(conn, jobId);
        conn.commit();
      } catch (Throwable e) {
        conn.rollback();
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized boolean hasMigrationJob(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException {
    return getMigrationJob(jobId, jobType, objectType, db, tbl) != null;
  }

  @Override
  public synchronized JobInfo getMigrationJob(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        return selectFromMmaTableMeta(conn, jobId, jobType, objectType, db, tbl);
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized List<JobInfo> listMigrationJobs(int limit) throws MmaException {
    return listMigrationJobsInternal(null, limit);
  }

  @Override
  public synchronized List<JobInfo> listMigrationJobs(
      MmaMetaManager.JobStatus status, int limit) throws MmaException {
    return listMigrationJobsInternal(status, limit);
  }

  @Override
  public synchronized List<RestoreJobInfo> listRestoreJobs(
      String condition, int limit) throws MmaException {

    try (Connection conn = ds.getConnection()) {
      try {
        return selectFromRestoreMeta(conn, condition, limit);
      } catch (Throwable e) {
        LOG.error(e);
        throw new MmaException("Failed to list restore jobs", e);
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }

  }

  @Override
  public synchronized void removeRestoreJob(String jobId) throws MmaException {
    try (Connection conn = ds.getConnection()) {
      String query = null;
      try (Statement stmt = conn.createStatement()) {
        query = String.format(
            "DELETE FROM %s WHERE %s='%s'",
            Constants.MMA_OBJ_RESTORE_TBL_NAME,
            Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID,
            jobId);

        stmt.execute(query);
        conn.commit();
      } catch (Throwable e) {
        LOG.error(e);
        throw new MmaException("Failed to remove restore job: " + query, e);
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized Map<String, List<String>> listTemporaryTables(
      String condition, int limit) throws MmaException {

    try (Connection conn = ds.getConnection()) {
      try {
        Map<String, List<String>> result = selectFromTemporaryTableMeta(conn, condition, limit);
        LOG.info("Temporary tables to be dropped: {}", GsonUtils.toJson(result));
        return result;
      } catch (Throwable e) {
        LOG.error(e);
        throw new MmaException("Failed to list restore jobs", e);
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public void removeTemporaryTableMeta(
      String jobId, String db, String tbl) throws MmaException {
    try (Connection conn = ds.getConnection()) {
      String query = null;
      try {
        query = String.format(
            "DELETE FROM %s WHERE %s='%s' AND %s='%s' AND %s='%s';",
            Constants.MMA_OBJ_TEMPORARY_TBL_NAME,
            Constants.MMA_OBJ_TEMPORARY_COL_UNIQUE_ID,
            jobId,
            "db",
            db,
            "tbl",
            tbl);
        LOG.info("Execute query: {}", query);
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(query);
          conn.commit();
        }
      } catch (Throwable e) {
        LOG.error(e);
        throw new MmaException("Failed to remove temporary table: " + query, e);
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  private List<JobInfo> listMigrationJobsInternal(
      MmaMetaManager.JobStatus status, int limit) throws MmaException {

    try (Connection conn = ds.getConnection()) {
      try {
        List<JobInfo> jobInfos = selectFromMmaTableMeta(conn, status, limit);
        List<JobInfo> ret = new LinkedList<>();

        if (jobInfos == null) {
          return ret;
        }

        for (JobInfo jobInfo : jobInfos) {
          if (status == null) {
            ret.add(jobInfo);
          } else {
            if (jobInfo.isPartitioned()) {
              String db = jobInfo.getDb();
              String tbl = jobInfo.getObject();
              MmaMetaManager.JobStatus realStatus = inferPartitionedTableStatus(
                  conn,
                  jobInfo.getJobId(),
                  jobInfo.getJobType().name(),
                  db,
                  tbl);
              if (status.equals(realStatus)) {
                ret.add(jobInfo);
              }
            } else if (status.equals(jobInfo.getStatus())) {
              ret.add(jobInfo);
            }
          }
        }

        conn.commit();
        return ret;
      } catch (Throwable e) {
        MmaException mmaException = MmaExceptionFactory.getFailedToListMigrationJobsException(e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public void updateStatus(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      MmaMetaManager.JobStatus status) throws MmaException {
    try (Connection conn = this.ds.getConnection()) {
      LOG.info("Enter updateStatus");
      if (db == null || tbl == null || status == null) {
        throw new IllegalArgumentException("'db' or 'tbl' or 'status' cannot be null");
      }
      db = db.toLowerCase();
      tbl = tbl.toLowerCase();
      updateStatusInternal(conn, jobId, jobType, objectType, db, tbl, status);
      conn.commit();
      LOG.info("Leave updateStatus");
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  private void updateStatusInternal(
      Connection conn,
      String uniqueId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      MmaMetaManager.JobStatus status) throws MmaException {
    try {
      JobInfo jobInfo = selectFromMmaTableMeta(conn, uniqueId, jobType, objectType, db, tbl);
      if (jobInfo == null) {
        LOG.info("Job not found, uniqueId {}, jobType {}, objectType {}, db {}, tbl {}",
                 uniqueId, jobType, objectType, db, tbl);
        throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
      }

      jobInfo.setStatus(status);
      // For a partitioned table, its migration status is inferred from its partitions' migration
      // statuses. And it does not have table level attr 'attemptTimes'.
      if (!jobInfo.isPartitioned()) {
        switch (status) {
          case SUCCEEDED: {
            jobInfo.setAttemptTimes(jobInfo.getAttemptTimes() + 1);
            break;
          }
          case FAILED: {
            int attemptTimes = jobInfo.getAttemptTimes() + 1;
            int retryTimesLimit =
                jobInfo.getJobConfig().getAdditionalTableConfig().getRetryTimesLimit();
            if (attemptTimes <= retryTimesLimit) {
              status = MmaMetaManager.JobStatus.PENDING;
            }
            jobInfo.setStatus(status);
            jobInfo.setAttemptTimes(attemptTimes);
            break;
          }
        }
      }
      mergeIntoMmaTableMeta(conn, jobInfo);
    } catch (Throwable e) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (Throwable e2) {
          LOG.error("Update migration job rollback failed, db: {}, tbl: {}", db, tbl);
        }
      }
      MmaException mmaException =
          MmaExceptionFactory.getFailedToUpdateMigrationJobException(db, tbl, e);
      LOG.error(e);
      throw mmaException;
    }
  }

  @Override
  public void updateStatus(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      List<List<String>> partitionValuesList,
      MmaMetaManager.JobStatus status) throws MmaException {

    try (Connection conn = this.ds.getConnection()) {
      LOG.info("Enter updateStatus");
      if (db == null || tbl == null || partitionValuesList == null || status == null) {
        throw new IllegalArgumentException("'db' or 'tbl' or 'partitionValuesList' or 'status' cannot be null");
      }

      db = db.toLowerCase();
      tbl = tbl.toLowerCase();
      updateStatusInternal(conn, jobId, jobType, objectType, db, tbl, partitionValuesList, status);
      conn.commit();
      LOG.info("Leave updateStatus");
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  private void updateStatusInternal(
      Connection conn,
      String uniqueId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      List<List<String>> partitionValuesList,
      MmaMetaManager.JobStatus status) throws MmaException {
    try {
      JobInfo jobInfo = selectFromMmaTableMeta(
          conn, uniqueId, jobType, objectType, db, tbl);
      if (jobInfo == null) {
        throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
      }

      List<MigrationJobPtInfo> newJobPtInfos = new LinkedList<>();
      for (List<String> partitionValues : partitionValuesList) {
        MigrationJobPtInfo jobPtInfo =
            selectFromMmaPartitionMeta(conn, uniqueId, jobType, db, tbl, partitionValues);
        if (jobPtInfo == null) {
          throw MmaExceptionFactory.getMigrationJobPtNotExistedException(db, tbl, partitionValues);
        }

        jobPtInfo.setStatus(status);
        switch (status) {
          case SUCCEEDED: {
            jobPtInfo.setAttemptTimes(jobPtInfo.getAttemptTimes() + 1);
            break;
          }
          case FAILED:
            int attemptTimes = jobPtInfo.getAttemptTimes() + 1;
            int retryTimesLimit = jobInfo
                .getJobConfig()
                .getAdditionalTableConfig()
                .getRetryTimesLimit();
            jobPtInfo.setStatus(status);
            if (attemptTimes <= retryTimesLimit) {
              jobPtInfo.setStatus(MmaMetaManager.JobStatus.PENDING);
            }
            jobPtInfo.setAttemptTimes(attemptTimes);
            break;
        }
        newJobPtInfos.add(jobPtInfo);
      }
      mergeIntoMmaPartitionMeta(conn, uniqueId, jobType, db, tbl, newJobPtInfos);

      // Update the table level status
      MmaMetaManager.JobStatus newStatus = inferPartitionedTableStatus(conn, uniqueId, jobType, db, tbl);
      if (!jobInfo.getStatus().equals(newStatus)) {
        updateStatusInternal(conn, uniqueId, jobType, objectType, db, tbl, newStatus);
      }
    } catch (Throwable e) {
      if (conn != null) {
        try {
          conn.rollback();
        } catch (Throwable e2) {
          LOG.error("Update migration job pt rollback failed, db: {}, tbl: {}", db, tbl);
        }
      }
      MmaException mmaException = MmaExceptionFactory.getFailedToUpdateMigrationJobException(db, tbl, e);
      LOG.error(e);
      throw mmaException;
    }
  }

  @Override
  public synchronized MmaMetaManager.JobStatus getStatus(
      String jobId,
      String jobType,
      String objectType,
      String db, String tbl) throws MmaException {
    LOG.info("Enter getStatus");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    return getStatusInternal(jobId, jobType, objectType, db, tbl);
  }

  private MmaMetaManager.JobStatus getStatusInternal(
      String jobId, String jobType, String objectType, String db, String tbl) throws MmaException {
    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = selectFromMmaTableMeta(conn, jobId, jobType, objectType, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }

        if (jobInfo.isPartitioned()) {
          return inferPartitionedTableStatus(
              conn,
              jobInfo.getJobId(),
              jobInfo.getJobType().name(),
              jobInfo.getDb(),
              jobInfo.getObject());
        }
        return jobInfo.getStatus();
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized MmaMetaManager.JobStatus getStatus(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      List<String> partitionValues) throws MmaException {
    LOG.info("Enter getStatus");

    if (db == null || tbl == null || partitionValues == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'partitionValues' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        MigrationJobPtInfo jobPtInfo = selectFromMmaPartitionMeta(
            conn, jobId, jobType, db, tbl, partitionValues);
        if (jobPtInfo == null) {
          throw MmaExceptionFactory.getMigrationJobPtNotExistedException(db, tbl, partitionValues);
        }
        return jobPtInfo.getStatus();
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobPtException(db, tbl, partitionValues);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized MmaMetaManager.MigrationProgress getProgress(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException {
    LOG.info("Enter getProgress");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = selectFromMmaTableMeta(conn, jobId, jobType, objectType, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }

        if (!jobInfo.isPartitioned()) {
          return null;
        }

        Map<MmaMetaManager.JobStatus, Integer> statusDistribution = getPartitionStatusDistribution(
            conn,
            jobInfo.getJobId(),
            jobInfo.getJobType().name(),
            db,
            tbl);
        int pending = statusDistribution.getOrDefault(JobStatus.PENDING, 0);
        int running = statusDistribution.getOrDefault(JobStatus.RUNNING, 0);
        int succeeded = statusDistribution.getOrDefault(JobStatus.SUCCEEDED, 0);
        int failed = statusDistribution.getOrDefault(JobStatus.FAILED, 0);

        return new MmaMetaManager.MigrationProgress(pending, running, succeeded, failed);
      } catch (Throwable e) {
        return null;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public MmaConfig.JobConfig getConfig(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException {
    LOG.info("Enter getConfig");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = selectFromMmaTableMeta(conn, jobId, jobType, objectType, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }
        return jobInfo.getJobConfig();
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobException(db, tbl, e);
        LOG.error(ExceptionUtils.getStackTrace(e));
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  private MetaSource getMetaSource(JobInfo jobInfo) {
    switch (jobInfo.getJobType()) {
      case MIGRATION: {
        TableMigrationConfig config = MmaConfig.TableMigrationConfig.fromJson(
            jobInfo.getJobConfig().getDescription());
        return MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());
      }
      case BACKUP: {
        ObjectBackupConfig config = MmaConfig.ObjectBackupConfig.fromJson(
            jobInfo.getJobConfig().getDescription());
        return MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());
      }
      case RESTORE: {
        if (ObjectType.DATABASE.equals(jobInfo.getObjectType())) {
            DatabaseRestoreConfig config = MmaConfig.DatabaseRestoreConfig
                .fromJson(jobInfo.getJobConfig().getDescription());
            return MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());
        } else {
          ObjectRestoreConfig config =
              MmaConfig.ObjectRestoreConfig.fromJson(jobInfo.getJobConfig().getDescription());
          return MetaSourceFactory.getOdpsMetaSource(config.getOdpsConfig());
        }
      }
    }

    throw new IllegalArgumentException("Unsupported job type");
  }

  @Override
  public List<MmaMetaManagerDbImplUtils.JobInfo> getPendingJobs() throws MmaException {
    LOG.info("Enter getPendingJobs");

    try (Connection conn = ds.getConnection()) {
      List<JobInfo> jobInfos =
          selectFromMmaTableMeta(conn, MmaMetaManager.JobStatus.PENDING, 3);
      for (JobInfo jobInfo : jobInfos) {
        String db = jobInfo.getDb();
        String object = jobInfo.getObject();

        MetaSource.TableMetaModel tableMetaModel;
        MetaSource metaSource = getMetaSource(jobInfo);
        try {
          MmaConfig.JobType jobType = jobInfo.getJobType();
          if (MmaConfig.JobType.BACKUP.equals(jobType)) {
            MmaConfig.ObjectBackupConfig config =
                MmaConfig.ObjectBackupConfig.fromJson(jobInfo.getJobConfig().getDescription());
            if (!MmaConfig.ObjectType.TABLE.equals(config.getObjectType())) {
              tableMetaModel = new MetaSource.TableMetaModel();
              tableMetaModel.databaseName = config.getDatabaseName();
              tableMetaModel.tableName = config.getObjectName();
              jobInfo.setTableMetaModel(tableMetaModel);
              continue;
            }
          } else if (MmaConfig.JobType.RESTORE.equals(jobType)) {
            tableMetaModel = new MetaSource.TableMetaModel();
            if (Strings.isNullOrEmpty(object)) {
              MmaConfig.DatabaseRestoreConfig config =
                  MmaConfig.DatabaseRestoreConfig.fromJson(jobInfo.getJobConfig().getDescription());
              tableMetaModel.databaseName = config.getSourceDatabaseName().toLowerCase();
              tableMetaModel.odpsProjectName = config.getDestinationDatabaseName().toLowerCase();
              tableMetaModel.tableName = null;
            } else {
              MmaConfig.ObjectRestoreConfig config =
                  MmaConfig.ObjectRestoreConfig.fromJson(jobInfo.getJobConfig().getDescription());
              tableMetaModel.databaseName = config.getSourceDatabaseName().toLowerCase();
              tableMetaModel.odpsProjectName = config.getDestinationDatabaseName().toLowerCase();
              tableMetaModel.tableName = config.getObjectName().toLowerCase();
              tableMetaModel.odpsTableName = config.getObjectName().toLowerCase();
            }
            jobInfo.setTableMetaModel(tableMetaModel);
            continue;
          }
          tableMetaModel = metaSource.getTableMetaWithoutPartitionMeta(db, object);
        } catch (Exception e) {
          // Table could be deleted after the task is submitted. In this case,
          // metaSource.getTableMetaWithoutPartitionMeta# will fail.
          LOG.warn("Failed to get metadata, db: {}, object: {}", db, object, e);
          updateStatusInternal(
              conn,
              jobInfo.getJobId(),
              jobInfo.getJobType().name(),
              jobInfo.getObjectType().name(),
              db,
              object,
              MmaMetaManager.JobStatus.FAILED);
          continue;
        }

        if (jobInfo.isPartitioned()) {
          List<MigrationJobPtInfo> jobPtInfos = selectFromMmaPartitionMeta(
              conn,
              jobInfo.getJobId(),
              jobInfo.getJobType().name(),
              db,
              object,
              MmaMetaManager.JobStatus.PENDING,
              -1);

          List<MetaSource.PartitionMetaModel> partitionMetaModels = new LinkedList<>();
          for (MigrationJobPtInfo jobPtInfo : jobPtInfos) {
            try {
              partitionMetaModels.add(
                  metaSource.getPartitionMeta(db, object, jobPtInfo.getPartitionValues()));
            } catch (Exception e) {
              // Partitions could be deleted after the task is submitted. In this case,
              // metaSource.getPartitionMeta# will fail.
              LOG.warn("Failed to get metadata, db: {}, object: {}, pt: {}",
                       db, object, jobPtInfo.getPartitionValues());
              updateStatusInternal(
                  conn,
                  jobInfo.getJobId(),
                  jobInfo.getJobType().name(),
                  jobInfo.getObjectType().name(),
                  db,
                  object,
                  Collections.singletonList(jobPtInfo.getPartitionValues()),
                  MmaMetaManager.JobStatus.FAILED);
            }
          }
          tableMetaModel.partitions = partitionMetaModels;
        }

        if (MmaConfig.JobType.MIGRATION.equals(jobInfo.getJobType())) {
          MmaConfig.TableMigrationConfig tableMigrationConfig =
              MmaConfig.TableMigrationConfig.fromJson(jobInfo.getJobConfig().getDescription());
          tableMigrationConfig.apply(tableMetaModel);
        } else if (MmaConfig.JobType.BACKUP.equals(jobInfo.getJobType())) {
          MmaConfig.ObjectBackupConfig objectBackupConfig =
              MmaConfig.ObjectBackupConfig.fromJson(jobInfo.getJobConfig().getDescription());
          objectBackupConfig.setDestTableStorage(ExternalTableStorage.OSS.name());
          objectBackupConfig.apply(tableMetaModel);
          tableMetaModel.odpsProjectName = objectBackupConfig.getDatabaseName().toLowerCase();
          tableMetaModel.odpsTableName = Constants.MMA_TEMPORARY_TABLE_PREFIX
              + objectBackupConfig.getObjectName() + "_" + objectBackupConfig.getBackupName();
        }
        jobInfo.setTableMetaModel(tableMetaModel);
      }
      conn.commit();
      jobInfos.sort(Comparator.comparing(job -> job.getDb() + job.getObject()));
      return jobInfos;
    }
    catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    } catch (Throwable e) {
      MmaException mmaException = MmaExceptionFactory.getFailedToGetPendingJobsException(e);
      LOG.error(e);
      throw mmaException;
    }
  }
}
