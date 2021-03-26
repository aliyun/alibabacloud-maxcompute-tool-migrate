package com.aliyun.odps.datacarrier.taskscheduler.meta;

import java.util.List;
import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.JobInfo;

public interface MmaMetaManager {

  void addMigrationJob(
      DataSource dataSource,
      String jobId,
      MmaConfig.TableMigrationConfig tableMigrationConfig) throws MmaException;

  void addObjectBackupJob(
      String jobId,
      MmaConfig.ObjectBackupConfig objectBackupConfig) throws MmaException;

  void addObjectRestoreJob(
      String jobId,
      MmaConfig.ObjectRestoreConfig objectRestoreConfig) throws MmaException;

  void addDatabaseRestoreJob(
      String jobId,
      MmaConfig.DatabaseRestoreConfig databaseRestoreConfig) throws MmaException;

  void mergeJobInfoIntoRestoreDB(
      MmaMetaManagerDbImplUtils.RestoreJobInfo paramRestoreJobInfo) throws MmaException;

  void updateStatusInRestoreDB(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String object,
      MmaMetaManager.JobStatus newStatus) throws MmaException;

  void removeMigrationJob(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String object) throws MmaException;

  void removeJob(String jobId) throws MmaException;

  boolean hasMigrationJob(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException;

  MmaMetaManagerDbImplUtils.JobInfo getMigrationJob(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException;

  List<JobInfo> listMigrationJobs(int limit) throws MmaException;

  List<MmaMetaManagerDbImplUtils.JobInfo> listMigrationJobs(
      MmaMetaManager.JobStatus status, int limit) throws MmaException;

  List<MmaMetaManagerDbImplUtils.RestoreJobInfo> listRestoreJobs(
      String condition, int limit) throws MmaException;

  void removeRestoreJob(String jobId) throws MmaException;

  Map<String, List<String>> listTemporaryTables(String condition, int limit) throws MmaException;

  void removeTemporaryTableMeta(
      String jobId, String db, String tbl) throws MmaException;

  void updateStatus(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      MmaMetaManager.JobStatus status) throws MmaException;

  void updateStatus(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      List<List<String>> partitionValuesList,
      MmaMetaManager.JobStatus status) throws MmaException;

  JobStatus getStatus(
      String jobId,
      String jobType,
      String objectType,
      String db, String tbl) throws MmaException;

  JobStatus getStatus(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl,
      List<String> partitionValues) throws MmaException;

  MigrationProgress getProgress(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException;

  MmaConfig.JobConfig getConfig(
      String jobId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws MmaException;

  List<MmaMetaManagerDbImplUtils.JobInfo> getPendingJobs() throws MmaException;

  void shutdown() throws MmaException;

  enum JobStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  class MigrationProgress {
    private int numPendingPartitions;
    private int numRunningPartitions;
    private int numSucceededPartitions;
    private int numFailedPartitions;

    public MigrationProgress(int numPendingPartitions, int numRunningPartitions, int numSucceededPartitions, int numFailedPartitions) {
      this.numPendingPartitions = numPendingPartitions;
      this.numRunningPartitions = numRunningPartitions;
      this.numSucceededPartitions = numSucceededPartitions;
      this.numFailedPartitions = numFailedPartitions;
    }

    public int getNumPendingPartitions() {
      return numPendingPartitions;
    }

    public int getNumRunningPartitions() {
      return numRunningPartitions;
    }

    public int getNumSucceededPartitions() {
      return numSucceededPartitions;
    }

    public int getNumFailedPartitions() {
      return numFailedPartitions;
    }

    public String toJson() {
      return GsonUtils.getFullConfigGson().toJson(this);
    }
  }
}
