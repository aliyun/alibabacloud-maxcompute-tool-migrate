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

import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager.JobStatus;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.base.Strings;
import com.google.gson.reflect.TypeToken;

public class MmaMetaManagerDbImplUtils {
  private static final Logger LOG = LogManager.getLogger(MmaMetaManagerDbImplUtils.class);

  private static String UPSERT_KEYWORD = null;

  // TODO: make it immutable
  public static class JobInfo {
    private String jobId;
    private MmaConfig.JobType jobType;
    private MmaConfig.ObjectType objectType;
    private String db;
    private String object;
    private boolean isPartitioned;
    private MmaConfig.JobConfig jobConfig;
    private MmaMetaManager.JobStatus status;
    private int attemptTimes;
    private Long lastModifiedTime;
    private MetaSource.TableMetaModel tableMetaModel;

    public JobInfo(
        String jobId,
        String jobType,
        String objectType,
        String db,
        String object,
        boolean isPartitioned,
        MmaConfig.JobConfig jobConfig,
        MmaMetaManager.JobStatus status,
        int attemptTimes,
        long lastModifiedTime) {
      this.jobId = Objects.requireNonNull(jobId);
      this.jobType = MmaConfig.JobType.valueOf(Objects.requireNonNull(jobType));
      this.objectType = MmaConfig.ObjectType.valueOf(Objects.requireNonNull(objectType));
      this.db = Objects.requireNonNull(db);
      this.object = Objects.requireNonNull(object);
      this.isPartitioned = isPartitioned;
      this.jobConfig = Objects.requireNonNull(jobConfig);
      this.status = Objects.requireNonNull(status);
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastModifiedTime;
    }

    public String getJobId() {
      return jobId;
    }

    public MmaConfig.JobType getJobType() {
      return jobType;
    }

    public MmaConfig.ObjectType getObjectType() {
      return objectType;
    }

    public String getDb() {
      return db;
    }

    public String getObject() {
      return object;
    }

    public boolean isPartitioned() {
      return isPartitioned;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public MmaMetaManager.JobStatus getStatus() {
      return status;
    }

    public MmaConfig.JobConfig getJobConfig() {
      return jobConfig;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public void setStatus(MmaMetaManager.JobStatus status) {
      this.status = status;
    }

    public void setAttemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }

    public void setTableMetaModel(MetaSource.TableMetaModel tableMetaModel) {
      this.tableMetaModel = tableMetaModel;
    }

    public MetaSource.TableMetaModel getTableMetaModel() {
      return tableMetaModel;
    }
  }

  public static class RestoreJobInfo {
    private String jobId;
    private MmaConfig.ObjectType objectType;
    private String db;
    private String object;
    private MmaConfig.JobConfig jobConfig;
    private MmaMetaManager.JobStatus status;
    private int attemptTimes;
    private long lastModifiedTime;

    public RestoreJobInfo(
        String jobId,
        String objectType,
        String db,
        String object,
        MmaConfig.JobConfig jobConfig,
        MmaMetaManager.JobStatus status,
        int attemptTimes,
        long lastModifiedTime) {
      this.jobId = jobId;
      this.objectType = MmaConfig.ObjectType.valueOf(objectType);
      this.db = db;
      this.object = object;
      this.jobConfig = jobConfig;
      this.status = status;
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastModifiedTime;
    }

    public String getJobId() {
      return jobId;
    }

    public MmaConfig.ObjectType getObjectType() {
      return objectType;
    }

    public String getDb() {
      return db;
    }

    public String getObject() {
      return object;
    }

    public MmaConfig.JobConfig getJobConfig() {
      return jobConfig;
    }

    public MmaMetaManager.JobStatus getStatus() {
      return status;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setStatus(MmaMetaManager.JobStatus status) {
      this.status = status;
    }

    public void setAttemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }
  }

  public static class MigrationJobPtInfo {
    private List<String> partitionValues;
    private MmaMetaManager.JobStatus status;
    private int attemptTimes;
    private Long lastModifiedTime;


    public MigrationJobPtInfo(
        List<String> partitionValues,
        MmaMetaManager.JobStatus status,
        int attemptTimes,
        long lastModifiedTime) {
      this.partitionValues = Objects.requireNonNull(partitionValues);
      this.status = Objects.requireNonNull(status);
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastModifiedTime;
    }

    public List<String> getPartitionValues() {
      return partitionValues;
    }

    public MmaMetaManager.JobStatus getStatus() {
      return status;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setStatus(MmaMetaManager.JobStatus status) {
      this.status = status;
    }

    public void setAttemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }
  }

  public static String getCreateMmaPartitionMetaSchemaDdl(String db) {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMd5(db));
    return "CREATE SCHEMA IF NOT EXISTS " + schemaName;
  }

  public static String getCreateMmaTableMetaDdl() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ")
      .append(Constants.MMA_OBJECT_META_TBL_NAME)
      .append(" (\n");

    for (Map.Entry<String, String> entry : Constants.MMA_TBL_META_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }

    sb.append("    PRIMARY KEY (").append(Constants.MMA_TBL_META_COL_UNIQUE_ID).append(", ");
    sb.append(Constants.MMA_TBL_META_COL_JOB_TYPE).append(", ");
    sb.append(Constants.MMA_TBL_META_COL_OBJECT_TYPE).append(", ");
    sb.append(Constants.MMA_TBL_META_COL_DB_NAME).append(", ");
    sb.append(Constants.MMA_TBL_META_COL_OBJECT_NAME).append("))\n");
    return sb.toString();
  }

  public static String getCreateMmaRestoreTableDdl() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ")
      .append(Constants.MMA_OBJ_RESTORE_TBL_NAME).append(" (\n");
    for (Map.Entry<String, String> entry : Constants.MMA_OBJ_RESTORE_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (")
      .append(Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID).append(", ")
      .append(Constants.MMA_OBJ_RESTORE_COL_OBJECT_TYPE).append(", ")
      .append(Constants.MMA_OBJ_RESTORE_COL_DB_NAME).append(", ")
      .append(Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME).append("))\n");
    return sb.toString();
  }

  public static String getCreateTemporaryTableDdl() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ")
      .append(Constants.MMA_OBJ_TEMPORARY_TBL_NAME).append(" (\n");
    for (Map.Entry<String, String> entry : Constants.MMA_OBJ_TEMPORARY_TBL_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (").append("unique_id").append(", ");
    sb.append(Constants.MMA_OBJ_TEMPORARY_COL_PROJECT).append(", ");
    sb.append(Constants.MMA_OBJ_TEMPORARY_COL_TABLE).append("))\n");
    return sb.toString();
  }

  public static String getCreateMmaPartitionMetaDdl(String db, String tbl) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ")
      .append(String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMd5(db)))
      .append(".")
      .append(String.format("MMA_PT_META_TBL_%s", getMd5(tbl)))
      .append(" (\n");

    for (Map.Entry<String, String> entry : Constants.MMA_PT_META_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (")
      .append(Constants.MMA_PT_META_COL_UNIQUE_ID).append(", ")
      .append(Constants.MMA_PT_META_COL_JOB_TYPE).append(", ")
      .append(Constants.MMA_PT_META_COL_PT_VALS).append("))\n");
    return sb.toString();
  }

  public static void createMmaTableMeta(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaTableMetaDdl();
      LOG.debug("Executing create table ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  public static void createMmaRestoreTable(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaRestoreTableDdl();
      LOG.debug("Executing create table ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  public static void createMmaTemporaryTable(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateTemporaryTableDdl();
      LOG.debug("Executing create table ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  public static void removeActiveTasksFromRestoreTable(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format(
          "DELETE FROM %s WHERE %s='%s' or %s='%s'",
          Constants.MMA_OBJ_RESTORE_TBL_NAME,
          Constants.MMA_OBJ_RESTORE_COL_STATUS,
          JobStatus.PENDING.name(),
          Constants.MMA_OBJ_RESTORE_COL_STATUS,
          JobStatus.RUNNING.name());
      LOG.debug("Executing delete rows ddl: {}", dml);
      stmt.execute(dml);
    }
  }

  public static void createMmaPartitionMetaSchema(Connection conn, String db) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaPartitionMetaSchemaDdl(db);
      LOG.debug("Executing create schema ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }



  public static void createMmaPartitionMeta(Connection conn, String db, String tbl) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaPartitionMetaDdl(db, tbl);
      LOG.debug("Executing create schema ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }


  private static String getUpsertKeyword() {
    if (UPSERT_KEYWORD == null) {
      UPSERT_KEYWORD =
          "mysql".equals(MmaServerConfig.getInstance().getMetaDbConfig().getDbType().toLowerCase())
              ? "REPLACE INTO " : "MERGE INTO ";
    }

    return UPSERT_KEYWORD;
  }

  public static void mergeIntoMmaTableMeta(Connection conn, JobInfo jobInfo) throws SQLException {
    String dml = getUpsertKeyword()
        + Constants.MMA_OBJECT_META_TBL_NAME
        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      int colIndex = 0;
      preparedStatement.setString(++colIndex, jobInfo.getJobId());
      preparedStatement.setString(++colIndex, jobInfo.getJobType().name());
      preparedStatement.setString(++colIndex, jobInfo.getObjectType().name());
      preparedStatement.setString(++colIndex, jobInfo.getDb());
      preparedStatement.setString(++colIndex, jobInfo.getObject());
      preparedStatement.setBoolean(++colIndex, jobInfo.isPartitioned());
      preparedStatement.setString(++colIndex,
                                  GsonUtils.getFullConfigGson().toJson(jobInfo.getJobConfig()));
      preparedStatement.setString(++colIndex, jobInfo.getStatus().toString());
      preparedStatement.setInt(++colIndex, jobInfo.getAttemptTimes());
      preparedStatement.setLong(++colIndex, jobInfo.getLastModifiedTime());
      assert colIndex == Constants.MMA_TBL_META_COL_TO_TYPE.size();

      LOG.debug("Executing DML: {}, arguments: {}",
                dml, GsonUtils.getFullConfigGson().toJson(jobInfo));
      preparedStatement.execute();
    }
  }


  public static void mergeIntoRestoreTableMeta(
      Connection conn,
      RestoreJobInfo taskInfo) throws SQLException {
    String dml = getUpsertKeyword()
        + Constants.MMA_OBJ_RESTORE_TBL_NAME
        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      preparedStatement.setString(1, taskInfo.getJobId());
      preparedStatement.setString(2, taskInfo.getObjectType().name());
      preparedStatement.setString(3, taskInfo.getDb());
      preparedStatement.setString(4, taskInfo.getObject());
      preparedStatement.setString(5,
                                  GsonUtils.getFullConfigGson().toJson(taskInfo.getJobConfig()));
      preparedStatement.setString(6, taskInfo.getStatus().toString());
      preparedStatement.setInt(7, taskInfo.getAttemptTimes());
      preparedStatement.setLong(8, taskInfo.getLastModifiedTime());

      LOG.debug("Executing DML: {}, arguments: {}",
                dml, GsonUtils.getFullConfigGson().toJson(taskInfo));
      preparedStatement.execute();
    }
  }


  public static void mergeIntoTemporaryTableMeta(Connection conn, String uniqueId, String db, String tbl) throws SQLException {
    String dml = getUpsertKeyword() + Constants.MMA_OBJ_TEMPORARY_TBL_NAME + " VALUES (?, ?, ?)";
    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      preparedStatement.setString(1, uniqueId);
      preparedStatement.setString(2, db);
      preparedStatement.setString(3, tbl);
      LOG.info("Executing DML: {}, arguments: ({}, {}, {})", dml, uniqueId, db, tbl);
      preparedStatement.execute();
    }
  }

  public static void deleteFromMmaMeta(Connection conn, String uniqueId, String jobType, String objectType, String db, String tbl) throws SQLException {
    String dml = String.format(
        "DELETE FROM %s WHERE %s='%s' and %s='%s' and %s='%s' and %s='%s'",
        Constants.MMA_OBJECT_META_TBL_NAME,
        Constants.MMA_TBL_META_COL_JOB_TYPE,
        jobType,
        Constants.MMA_TBL_META_COL_OBJECT_TYPE,
        objectType,
        Constants.MMA_TBL_META_COL_DB_NAME,
        db,
        Constants.MMA_TBL_META_COL_OBJECT_NAME,
        tbl);

    if (uniqueId != null) {
      dml = String.format("%s and %s='%s'", dml, Constants.MMA_TBL_META_COL_UNIQUE_ID, uniqueId);
    }
    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing DML: {}", dml);
      stmt.execute(dml);
    }
  }

  public static void deleteFromMmaMeta(Connection conn, String jobId) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format(
          "DELETE FROM %s WHERE %s='%s'",
          Constants.MMA_OBJECT_META_TBL_NAME,
          Constants.MMA_TBL_META_COL_UNIQUE_ID,
          jobId);

      LOG.debug("Executing DML: {}", dml);
      stmt.execute(dml);

      // TODO: doesn't make any sense!
      dml = String.format(
          "DELETE FROM %s WHERE %s='%s'",
          Constants.MMA_OBJ_RESTORE_TBL_NAME,
          Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID,
          jobId);
      LOG.debug("Executing DML: {}", dml);
      stmt.execute(dml);
    }
  }

  public static JobInfo selectFromMmaTableMeta(
      Connection conn,
      String uniqueId,
      String jobType,
      String objectType,
      String db,
      String tbl) throws SQLException {
    String sql = String.format(
        "SELECT * FROM %s WHERE %s='%s' and %s='%s' and %s='%s' and %s='%s'",
        Constants.MMA_OBJECT_META_TBL_NAME,
        Constants.MMA_TBL_META_COL_JOB_TYPE,
        jobType,
        Constants.MMA_TBL_META_COL_OBJECT_TYPE,
        objectType,
        Constants.MMA_TBL_META_COL_DB_NAME,
        db,
        Constants.MMA_TBL_META_COL_OBJECT_NAME,
        tbl);

    if (uniqueId != null) {
      sql = String.format("%s and %s='%s'", sql, Constants.MMA_TBL_META_COL_UNIQUE_ID, uniqueId);
    }
    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if(!rs.next()) {
          return null;
        }
        return new JobInfo(
            uniqueId,
            jobType,
            objectType,
            db,
            tbl,
            rs.getBoolean(Constants.MMA_TBL_META_COL_IS_PARTITIONED),
            GsonUtils.getFullConfigGson().fromJson(rs.getString(Constants.MMA_TBL_META_COL_MIGRATION_CONF), MmaConfig.JobConfig.class),
            JobStatus.valueOf(rs.getString(Constants.MMA_TBL_META_COL_STATUS)),
            rs.getInt(Constants.MMA_TBL_META_COL_ATTEMPT_TIMES),
            rs.getLong(Constants.MMA_TBL_META_COL_LAST_MODIFIED_TIME));
      }
    }
  }

  public static List<JobInfo> selectFromMmaTableMeta(
      Connection conn,
      MmaMetaManager.JobStatus status,
      int limit) throws SQLException {

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT * FROM %s", Constants.MMA_OBJECT_META_TBL_NAME));
    if (status != null) {
      sb.append(String.format(
          " WHERE %s='%s'", Constants.MMA_TBL_META_COL_STATUS, status.toString()));
    }
    sb.append(String.format(
        " ORDER BY %s, %s, %s, %s, %s DESC",
        Constants.MMA_TBL_META_COL_UNIQUE_ID,
        Constants.MMA_TBL_META_COL_JOB_TYPE,
        Constants.MMA_TBL_META_COL_OBJECT_TYPE,
        Constants.MMA_TBL_META_COL_DB_NAME,
        Constants.MMA_TBL_META_COL_OBJECT_NAME));

    if (limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    String sql = sb.toString();

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sql);

      List<JobInfo> jobInfos = new LinkedList<>();
      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          JobInfo jobInfo = new JobInfo(
              rs.getString(Constants.MMA_TBL_META_COL_UNIQUE_ID),
              rs.getString(Constants.MMA_TBL_META_COL_JOB_TYPE),
              rs.getString(Constants.MMA_TBL_META_COL_OBJECT_TYPE),
              rs.getString(Constants.MMA_TBL_META_COL_DB_NAME),
              rs.getString(Constants.MMA_TBL_META_COL_OBJECT_NAME),
              rs.getBoolean(Constants.MMA_TBL_META_COL_IS_PARTITIONED),
              GsonUtils.getFullConfigGson()
                       .fromJson(rs.getString(Constants.MMA_TBL_META_COL_MIGRATION_CONF),
                                 MmaConfig.JobConfig.class),
              JobStatus.valueOf(rs.getString(Constants.MMA_TBL_META_COL_STATUS)),
              rs.getInt(Constants.MMA_TBL_META_COL_ATTEMPT_TIMES),
              rs.getLong(Constants.MMA_TBL_META_COL_LAST_MODIFIED_TIME));
          jobInfos.add(jobInfo);
        }
      }

      return jobInfos;
    }
  }


  public static List<RestoreJobInfo> selectFromRestoreMeta(Connection conn, String condition, int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT * FROM %s\n", Constants.MMA_OBJ_RESTORE_TBL_NAME));
    if (!Strings.isNullOrEmpty(condition)) {
      sb.append(condition).append("\n");
    }
    sb.append(String.format(
        "ORDER BY %s, %s, %s, %s DESC\n",
        Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID,
        Constants.MMA_OBJ_RESTORE_COL_DB_NAME,
        Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME,
        Constants.MMA_OBJ_RESTORE_COL_OBJECT_TYPE));

    if (limit > 0) {
      sb.append("LIMIT ").append(limit);
    }
    sb.append(";");

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sb.toString());
      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        List<RestoreJobInfo> ret = new LinkedList<>();
        while (rs.next()) {
          RestoreJobInfo restoreJobInfo = new RestoreJobInfo(
              rs.getString(Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID),
              rs.getString(Constants.MMA_TBL_META_COL_OBJECT_TYPE),
              rs.getString(Constants.MMA_OBJ_RESTORE_COL_DB_NAME),
              rs.getString(Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME),
              GsonUtils.getFullConfigGson().fromJson(rs.getString(Constants.MMA_OBJ_RESTORE_COL_JOB_CONFIG), MmaConfig.JobConfig.class),
              JobStatus.valueOf(rs.getString(Constants.MMA_OBJ_RESTORE_COL_STATUS)),
              rs.getInt(Constants.MMA_OBJ_RESTORE_COL_ATTEMPT_TIMES),
              rs.getLong(Constants.MMA_OBJ_RESTORE_COL_LAST_MODIFIED_TIME));
          ret.add(restoreJobInfo);
        }
        return ret;
      }
    }
  }

  public static Map<String, List<String>> selectFromTemporaryTableMeta(Connection conn, String condition, int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT * FROM %s\n", Constants.MMA_OBJ_TEMPORARY_TBL_NAME));
    if (!Strings.isNullOrEmpty(condition)) {
      sb.append(condition).append("\n");
    }
    if (limit > 0) {
      sb.append("LIMIT ").append(limit);
    }
    sb.append(";");

    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sb.toString());
      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        Map<String, List<String>> ret = new HashMap<>();
        while (rs.next()) {
          String project = rs.getString(2);
          String table = rs.getString(3);
          List<String> tables = ret.computeIfAbsent(project, k -> new ArrayList<>());
          tables.add(table);
        }
        return ret;
      }
    }
  }


  public static void mergeIntoMmaPartitionMeta(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl,
      List<MigrationJobPtInfo> migrationJobPtInfos) throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMd5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMd5(tbl));
    String dml = getUpsertKeyword() + schemaName + "." + tableName + " VALUES(?, ?, ?, ?, ?, ?)";

    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      for (MigrationJobPtInfo jobPtInfo : migrationJobPtInfos) {

        String partitionValuesJson = GsonUtils.getFullConfigGson().toJson(jobPtInfo.getPartitionValues());
        int columnIndex = 0;
        preparedStatement.setString(++columnIndex, uniqueId);
        preparedStatement.setString(++columnIndex, jobType);
        preparedStatement.setString(++columnIndex, partitionValuesJson);
        preparedStatement.setString(++columnIndex, jobPtInfo.getStatus().toString());
        preparedStatement.setInt(++columnIndex, jobPtInfo.getAttemptTimes());
        preparedStatement.setLong(++columnIndex, jobPtInfo.getLastModifiedTime());
        preparedStatement.addBatch();
        assert columnIndex == Constants.MMA_PT_META_COL_TO_TYPE.size();
        LOG.debug("Executing DML: {}, arguments: {}",
                  dml, GsonUtils.getFullConfigGson().toJson(jobPtInfo));
      }

      preparedStatement.executeBatch();
    }
  }

  public static void dropMmaPartitionMeta(
      Connection conn,
      String db,
      String tbl) throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMd5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMd5(tbl));

    String ddl = "DROP TABLE " + schemaName + "." + tableName;
    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing DDL: {}", ddl);

      stmt.execute(ddl);
    }
  }

  public static MigrationJobPtInfo selectFromMmaPartitionMeta(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl,
      List<String> partitionValues) throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMd5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMd5(tbl));
    String sql = String.format(
        "SELECT * FROM %s.%s WHERE %s='%s' and %s='%s' and %s='%s'",
        schemaName,
        tableName,
        Constants.MMA_PT_META_COL_UNIQUE_ID,
        uniqueId,
        Constants.MMA_PT_META_COL_JOB_TYPE,
        jobType,
        Constants.MMA_PT_META_COL_PT_VALS,
        GsonUtils.getFullConfigGson().toJson(partitionValues));

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if (!rs.next()) {
          return null;
        }
        return new MigrationJobPtInfo(
            partitionValues,
            JobStatus.valueOf(rs.getString(Constants.MMA_PT_META_COL_STATUS)),
            rs.getInt(Constants.MMA_PT_META_COL_ATTEMPT_TIMES),
            rs.getLong(Constants.MMA_PT_META_COL_LAST_MODIFIED_TIME));
      }
    }
  }

  public static List<MigrationJobPtInfo> selectFromMmaPartitionMeta(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl,
      MmaMetaManager.JobStatus status,
      int limit) throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMd5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMd5(tbl));

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM ").append(schemaName).append(".").append(tableName);
    sb.append(String.format(
        " WHERE %s='%s' AND %s='%s'",
        Constants.MMA_PT_META_COL_UNIQUE_ID,
        uniqueId,
        Constants.MMA_PT_META_COL_JOB_TYPE,
        jobType));

    if (status != null) {
      sb.append(String.format(
          " AND %s='%s'", Constants.MMA_PT_META_COL_STATUS, status.toString()));
    }
    sb.append(" ORDER BY ")
      .append(Constants.MMA_PT_META_COL_UNIQUE_ID).append(", ")
      .append(Constants.MMA_PT_META_COL_JOB_TYPE).append(", ")
      .append(Constants.MMA_PT_META_COL_PT_VALS);
    if (limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    Type type = new TypeToken<List<String>>() {}.getType();
    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sb.toString());

      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        List<MigrationJobPtInfo> ret = new LinkedList<>();
        while (rs.next()) {
          ret.add(new MigrationJobPtInfo(
              GsonUtils.getFullConfigGson().fromJson(rs.getString(Constants.MMA_PT_META_COL_PT_VALS), type),
              JobStatus.valueOf(rs.getString(Constants.MMA_PT_META_COL_STATUS)),
              rs.getInt(Constants.MMA_PT_META_COL_ATTEMPT_TIMES),
              rs.getLong(Constants.MMA_PT_META_COL_LAST_MODIFIED_TIME)));
        }
        return ret;
      }
    }
  }

  public static Map<MmaMetaManager.JobStatus, Integer> getPartitionStatusDistribution(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl) throws SQLException {
    String schemaName = String.format("MMA_PT_META_DB_%s", getMd5(db));
    String tableName = String.format("MMA_PT_META_TBL_%s", getMd5(tbl));

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ")
      .append(Constants.MMA_PT_META_COL_STATUS).append(", COUNT(1) as CNT FROM ")
      .append(schemaName).append(".").append(tableName)
      .append(" WHERE ")
      .append(Constants.MMA_PT_META_COL_UNIQUE_ID).append("='")
      .append(uniqueId).append("' AND ")
      .append(Constants.MMA_PT_META_COL_JOB_TYPE).append("='")
      .append(jobType).append("'")
      .append(" GROUP BY ").append(Constants.MMA_PT_META_COL_STATUS);

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sb.toString());
      Map<JobStatus, Integer> ret = new HashMap<>();
      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        while (rs.next()) {
          JobStatus status = JobStatus.valueOf(rs.getString(1));
          Integer count = rs.getInt(2);
          ret.put(status, count);
        }

        return ret;
      }
    }
  }

  public static List<List<String>> filterOutPartitions(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl,
      List<List<String>> candidates) throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMd5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMd5(tbl));

    String sql = String.format(
        "SELECT %s FROM %s.%s WHERE %s='%s' AND %s='%s' AND %s='%s'",
        Constants.MMA_PT_META_COL_PT_VALS,
        schemaName,
        tableName,
        Constants.MMA_PT_META_COL_UNIQUE_ID,
        uniqueId,
        Constants.MMA_PT_META_COL_JOB_TYPE,
        jobType,
        Constants.MMA_PT_META_COL_STATUS,
        JobStatus.SUCCEEDED.name());

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        Set<String> managedPartitionValuesJsonSet = new HashSet<>();
        while (rs.next()) {
          managedPartitionValuesJsonSet.add(rs.getString(1));
        }

        Type type = new TypeToken<List<String>>() {}.getType();

        // Filter out existing partitions
        return candidates
            .stream()
            .map(ptv -> GsonUtils.getFullConfigGson().toJson(ptv))
            .filter(v -> !managedPartitionValuesJsonSet.contains(v))
            .map(json -> (List<String>) GsonUtils.getFullConfigGson().fromJson(json, type))
            .collect(Collectors.toList());
      }
    }
  }

  public static MmaMetaManager.JobStatus inferPartitionedTableStatus(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl) throws SQLException {

    Map<MmaMetaManager.JobStatus, Integer> statusDistribution =
        getPartitionStatusDistribution(conn, uniqueId, jobType, db, tbl);
    int total = statusDistribution.values().stream().reduce(0, Integer::sum);
    int pending = statusDistribution.getOrDefault(JobStatus.PENDING, 0);
    int succeeded = statusDistribution.getOrDefault(JobStatus.SUCCEEDED, 0);
    int failed = statusDistribution.getOrDefault(JobStatus.FAILED, 0);

    if (total == succeeded) {
      return MmaMetaManager.JobStatus.SUCCEEDED;
    }
    if (total == succeeded + failed && failed != 0) {
      return MmaMetaManager.JobStatus.FAILED;
    }
    if (total == pending + succeeded + failed && pending != 0) {
      return MmaMetaManager.JobStatus.PENDING;
    }
    return MmaMetaManager.JobStatus.RUNNING;
  }

  public static String generateMigrationUniqueId(
      String sourceDB,
      String sourceTbl,
      String destinationDB,
      String destinationTbl) {
    if (StringUtils.isNullOrEmpty(sourceDB) ||
        StringUtils.isNullOrEmpty(sourceTbl) ||
        StringUtils.isNullOrEmpty(destinationDB) ||
        StringUtils.isNullOrEmpty(destinationTbl)) {
      LOG.error("Generate migration unique id failed, source db {}, source tb {}, destination db {}, destination tb {}", sourceDB, sourceTbl, destinationDB, destinationTbl);

      throw new RuntimeException("Generate migration unique id failed");
    }
    return sourceDB.toLowerCase() + "." + sourceTbl.toLowerCase() + ":" + destinationDB
        .toLowerCase() + "." + destinationTbl.toLowerCase();
  }

  public static String getMd5(String content) {
    return Hex.encodeHexString(DigestUtils.md5(content));
  }
}
