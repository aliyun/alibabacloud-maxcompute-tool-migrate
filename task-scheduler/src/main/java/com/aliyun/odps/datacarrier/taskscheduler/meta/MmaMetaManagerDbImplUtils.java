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

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager.MigrationStatus;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.google.gson.reflect.TypeToken;

public class MmaMetaManagerDbImplUtils {

  private static final Logger LOG = LogManager.getLogger(MmaMetaManagerDbImplUtils.class);

  private static String UPSERT_KEYWORD = null;

  private static String[] hexArray = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};

  /**
   * Represents a row in table meta
   */
  public static class JobInfo {
    private String uniqueId;
    private String jobType;
    private String objectType;
    private String db;
    private String tbl;
    private boolean isPartitioned;
    private MmaConfig.JobConfig jobConfig;
    private MigrationStatus status;
    private int attemptTimes;
    private Long lastModifiedTime;
    private MetaSource.TableMetaModel tableMetaModel;

    public JobInfo(String uniqueId,
                   String jobType,
                   String objectType,
                   String db,
                   String tbl,
                   boolean isPartitioned,
                   MmaConfig.JobConfig jobConfig,
                   MigrationStatus status,
                   int attemptTimes,
                   long lastModifiedTime) {
      this.uniqueId = Objects.requireNonNull(uniqueId);
      this.jobType = Objects.requireNonNull(jobType);
      this.objectType = Objects.requireNonNull(objectType);
      this.db = Objects.requireNonNull(db);
      this.tbl = Objects.requireNonNull(tbl);
      this.isPartitioned = isPartitioned;
      this.jobConfig = Objects.requireNonNull(jobConfig);
      this.status = Objects.requireNonNull(status);
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastModifiedTime;
    }

    public String getUniqueId() {
      return uniqueId;
    }

    public String getJobType() {
      return jobType;
    }

    public String getObjectType() {
      return objectType;
    }

    public String getDb() {
      return db;
    }

    public String getTbl() {
      return tbl;
    }

    public boolean isPartitioned() {
      return isPartitioned;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public MigrationStatus getStatus() {
      return status;
    }

    public MmaConfig.JobConfig getJobConfig() {
      return jobConfig;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public void setStatus(MigrationStatus status) {
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

  public static class RestoreTaskInfo {
    private String uniqueId;
    private String type;
    private String db;
    private String object;
    private MmaConfig.JobConfig jobConfig;
    private MigrationStatus status;
    private int attemptTimes;
    private long lastModifiedTime;

    public RestoreTaskInfo(String uniqueId,
                           String type,
                           String db,
                           String object,
                           MmaConfig.JobConfig jobConfig,
                           MigrationStatus status,
                           int attemptTimes,
                           long lastModifiedTime) {
      this.uniqueId = uniqueId;
      this.type = type;
      this.db = db;
      this.object = object;
      this.jobConfig = jobConfig;
      this.status = status;
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastModifiedTime;
    }

    public String getUniqueId() {
      return uniqueId;
    }

    public String getType() {
      return type;
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

    public MigrationStatus getStatus() {
      return status;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setStatus(MigrationStatus status) {
      this.status = status;
    }

    public void setAttemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }
  }

  /**
   * Represents a row in partition meta
   */
  public static class MigrationJobPtInfo {
    private List<String> partitionValues;
    private MigrationStatus status;
    private int attemptTimes;
    private Long lastModifiedTime;

    public MigrationJobPtInfo(List<String> partitionValues,
                              MigrationStatus status,
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

    public MigrationStatus getStatus() {
      return status;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setStatus(MigrationStatus status) {
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
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db));
    return "CREATE SCHEMA IF NOT EXISTS " + schemaName;
  }

  public static String getCreateMmaTableMetaDdl() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(Constants.MMA_OBJECT_META_TBL_NAME).append(" (\n");
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
    sb.append("CREATE TABLE IF NOT EXISTS ").append(Constants.MMA_OBJ_RESTORE_TBL_NAME).append(" (\n");
    for (Map.Entry<String, String> entry : Constants.MMA_OBJ_RESTORE_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (").append(Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID).append(", ");
    sb.append(Constants.MMA_OBJ_RESTORE_COL_TYPE).append(", ");
    sb.append(Constants.MMA_OBJ_RESTORE_COL_DB_NAME).append(", ");
    sb.append(Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME).append("))\n");
    return sb.toString();
  }

  public static String getCreateTemporaryTableDdl() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(Constants.MMA_OBJ_TEMPORARY_TBL_NAME).append(" (\n");
    for (Map.Entry<String, String> entry : Constants.MMA_OBJ_TEMPORARY_TBL_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (").append(Constants.MMA_OBJ_TEMPORARY_COL_UNIQUE_ID).append(", ");
    sb.append(Constants.MMA_OBJ_TEMPORARY_COL_PROJECT).append(", ");
    sb.append(Constants.MMA_OBJ_TEMPORARY_COL_TABLE).append("))\n");
    return sb.toString();
  }

  public static String getCreateMmaPartitionMetaDdl(String db, String tbl) {
    StringBuilder sb = new StringBuilder();
    sb
        .append("CREATE TABLE IF NOT EXISTS ")
        .append(String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db)))
        .append(".")
        .append(String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMD5(tbl)))
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
      String dml = String.format("DELETE FROM %s WHERE %s='%s' or %s='%s'",
          Constants.MMA_OBJ_RESTORE_TBL_NAME,
          Constants.MMA_OBJ_RESTORE_COL_STATUS,
          MigrationStatus.PENDING.name(),
          Constants.MMA_OBJ_RESTORE_COL_STATUS,
          MigrationStatus.RUNNING.name());
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

  public static void createMmaPartitionMeta(Connection conn,
                                            String db,
                                            String tbl) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaPartitionMetaDdl(db, tbl);
      LOG.debug("Executing create schema ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  // return MERGE INTO or REPLACE INTO
  private synchronized static String getUpsertKeyword() {
    if (UPSERT_KEYWORD == null) {
      UPSERT_KEYWORD = MmaServerConfig.getInstance().getMetaDBConfig().getDbType().toLowerCase().equals("mysql") ?
          "REPLACE INTO " : "MERGE INTO ";
    }
    return UPSERT_KEYWORD;
  }

  /**
   * Insert into or update (A.K.A Upsert) MMA_TBL_META
   */
  public static void mergeIntoMmaTableMeta(Connection conn, JobInfo jobInfo)
      throws SQLException {

    String dml = getUpsertKeyword() + Constants.MMA_OBJECT_META_TBL_NAME + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      int colIndex = 0;
      preparedStatement.setString(++colIndex, jobInfo.getUniqueId());
      preparedStatement.setString(++colIndex, jobInfo.getJobType());
      preparedStatement.setString(++colIndex, jobInfo.getObjectType());
      preparedStatement.setString(++colIndex, jobInfo.getDb());
      preparedStatement.setString(++colIndex, jobInfo.getTbl());
      preparedStatement.setBoolean(++colIndex, jobInfo.isPartitioned());
      preparedStatement.setString(++colIndex, GsonUtils.getFullConfigGson().toJson(jobInfo.getJobConfig()));
      preparedStatement.setString(++colIndex, jobInfo.getStatus().toString());
      preparedStatement.setInt(++colIndex, jobInfo.getAttemptTimes());
      preparedStatement.setLong(++colIndex, jobInfo.getLastModifiedTime());
      assert colIndex == Constants.MMA_TBL_META_COL_TO_TYPE.size();

      LOG.debug("Executing DML: {}, arguments: {}",
               dml,
               GsonUtils.getFullConfigGson().toJson(jobInfo));

      preparedStatement.execute();
    }
  }

  public static void mergeIntoRestoreTableMeta(Connection conn, RestoreTaskInfo taskInfo)
      throws SQLException {
    String dml = getUpsertKeyword() + Constants.MMA_OBJ_RESTORE_TBL_NAME + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      preparedStatement.setString(1, taskInfo.getUniqueId());
      preparedStatement.setString(2, taskInfo.getType());
      preparedStatement.setString(3, taskInfo.getDb());
      preparedStatement.setString(4, taskInfo.getObject());
      preparedStatement.setString(5,
                                  GsonUtils.getFullConfigGson().toJson(taskInfo.getJobConfig()));
      preparedStatement.setString(6, taskInfo.getStatus().toString());
      preparedStatement.setInt(7, taskInfo.getAttemptTimes());
      preparedStatement.setLong(8, taskInfo.getLastModifiedTime());

      LOG.debug("Executing DML: {}, arguments: {}",
               dml,
               GsonUtils.getFullConfigGson().toJson(taskInfo));

      preparedStatement.execute();
    }
  }

  public static void mergeIntoTemporaryTableMeta(Connection conn, String uniqueId, String db, String tbl)
      throws SQLException {
    String dml = getUpsertKeyword() + Constants.MMA_OBJ_TEMPORARY_TBL_NAME + " VALUES (?, ?, ?)";
    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      preparedStatement.setString(1, uniqueId);
      preparedStatement.setString(2, db);
      preparedStatement.setString(3, tbl);
      LOG.info("Executing DML: {}, arguments: ({}, {}, {})", dml, uniqueId, db, tbl);
      preparedStatement.execute();
    }
  }

  /**
   * Delete from MMA_META
   * for compatibility, uniqueId maybe null
   */
  public static void deleteFromMmaMeta(Connection conn,
                                       String uniqueId,
                                       String jobType,
                                       String objectType,
                                       String db,
                                       String tbl) throws SQLException {
    String dml = String.format("DELETE FROM %s WHERE %s='%s' and %s='%s' and %s='%s' and %s='%s'",
                               Constants.MMA_OBJECT_META_TBL_NAME,
                               Constants.MMA_TBL_META_COL_JOB_TYPE, jobType,
                               Constants.MMA_TBL_META_COL_OBJECT_TYPE, objectType,
                               Constants.MMA_TBL_META_COL_DB_NAME, db,
                               Constants.MMA_TBL_META_COL_OBJECT_NAME, tbl);
    if (uniqueId != null) {
      dml = String.format("%s and %s='%s'", dml, Constants.MMA_TBL_META_COL_UNIQUE_ID, uniqueId);
    }
    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing DML: {}", dml);
      stmt.execute(dml);
    }
  }

  /**
   * Return a record from MMA_TBL_META if it exists, else null
   * for compatibility, uniqueId maybe null
   */
  public static JobInfo selectFromMmaTableMeta(Connection conn,
                                               String uniqueId,
                                               String jobType,
                                               String objectType,
                                               String db,
                                               String tbl) throws SQLException {
    String sql = String.format("SELECT * FROM %s WHERE %s='%s' and %s='%s' and %s='%s' and %s='%s'",
                               Constants.MMA_OBJECT_META_TBL_NAME,
                               Constants.MMA_TBL_META_COL_JOB_TYPE, jobType,
                               Constants.MMA_TBL_META_COL_OBJECT_TYPE, objectType,
                               Constants.MMA_TBL_META_COL_DB_NAME, db,
                               Constants.MMA_TBL_META_COL_OBJECT_NAME, tbl);

    if (uniqueId != null) {
      sql = String.format("%s and %s='%s'", sql, Constants.MMA_TBL_META_COL_UNIQUE_ID, uniqueId);
    }
    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if(!rs.next()) {
          return null;
        }
        return new JobInfo(rs.getString(1),
                           rs.getString(2),
                           rs.getString(3),
                           rs.getString(4),
                           rs.getString(5),
                           rs.getBoolean(6),
                           GsonUtils.getFullConfigGson().fromJson(rs.getString(7), MmaConfig.JobConfig.class),
                           MigrationStatus.valueOf(rs.getString(8)),
                           rs.getInt(9),
                           rs.getLong(10));
      }
    }
  }

  /**
   * Return records from MMA_TBL_META
   */
  public static List<JobInfo> selectFromMmaTableMeta(Connection conn,
                                                     MigrationStatus status,
                                                     int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT * FROM %s", Constants.MMA_OBJECT_META_TBL_NAME));
    if (status != null) {
      sb.append(String.format(" WHERE %s='%s'",
          Constants.MMA_PT_META_COL_STATUS,
          status.toString()));
    }
    sb.append(String.format(" ORDER BY %s, %s, %s, %s, %s DESC",
        Constants.MMA_TBL_META_COL_UNIQUE_ID,
        Constants.MMA_TBL_META_COL_JOB_TYPE,
        Constants.MMA_TBL_META_COL_OBJECT_TYPE,
        Constants.MMA_TBL_META_COL_DB_NAME,
        Constants.MMA_TBL_META_COL_OBJECT_NAME));
    if (limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sb.toString());

      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        List<JobInfo> ret = new LinkedList<>();
        while (rs.next()) {
          int columnIndex = 0;
          JobInfo jobInfo = new JobInfo(rs.getString(++columnIndex),
                                        rs.getString(++columnIndex),
                                        rs.getString(++columnIndex),
                                        rs.getString(++columnIndex),
                                        rs.getString(++columnIndex),
                                        rs.getBoolean(++columnIndex),
                                        GsonUtils.getFullConfigGson().fromJson(rs.getString(++columnIndex), MmaConfig.JobConfig.class),
                                        MigrationStatus.valueOf(rs.getString(++columnIndex)),
                                        rs.getInt(++columnIndex),
                                        rs.getLong(++columnIndex));
          assert columnIndex == Constants.MMA_TBL_META_COL_TO_TYPE.size();
          ret.add(jobInfo);
        }
        return ret;
      }
    }
  }

  public static List<RestoreTaskInfo> selectFromRestoreMeta(Connection conn,
                                                            String condition,
                                                            int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT * FROM %s\n", Constants.MMA_OBJ_RESTORE_TBL_NAME));
    if (!Strings.isNullOrEmpty(condition)) {
      sb.append(condition).append("\n");
    }
    sb.append(String.format("ORDER BY %s, %s, %s, %s DESC\n",
                            Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID,
                            Constants.MMA_OBJ_RESTORE_COL_DB_NAME,
                            Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME,
                            Constants.MMA_OBJ_RESTORE_COL_TYPE));
    if (limit > 0) {
      sb.append("LIMIT ").append(limit);
    }
    sb.append(";");

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sb.toString());
      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        List<RestoreTaskInfo> ret = new LinkedList<>();
        while (rs.next()) {
          RestoreTaskInfo taskInfo = new RestoreTaskInfo(
              rs.getString(1),
              rs.getString(2),
              rs.getString(3),
              rs.getString(4),
              GsonUtils.getFullConfigGson().fromJson(rs.getString(5), MmaConfig.JobConfig.class),
              MigrationStatus.valueOf(rs.getString(6)),
              rs.getInt(7),
              rs.getLong(8));
          ret.add(taskInfo);
        }
        return ret;
      }
    }
  }

  public static Map<String, List<String>> selectFromTemporaryTableMeta(Connection conn,
                                                                       String condition,
                                                                       int limit) throws SQLException {
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

  /**
   * Insert into or update (A.K.A Upsert) MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl]
   */
  public static void mergeIntoMmaPartitionMeta(Connection conn,
                                               String uniqueId,
                                               String jobType,
                                               String db,
                                               String tbl,
                                               List<MigrationJobPtInfo> migrationJobPtInfos)
      throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMD5(tbl));
    String dml = getUpsertKeyword() + schemaName + "." + tableName + " VALUES(?, ?, ?, ?, ?, ?)";

    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      for (MigrationJobPtInfo jobPtInfo : migrationJobPtInfos) {
        String partitionValuesJson =
            GsonUtils.getFullConfigGson().toJson(jobPtInfo.getPartitionValues());
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
                 dml,
                 GsonUtils.getFullConfigGson().toJson(jobPtInfo));
      }

      preparedStatement.executeBatch();
    }
  }

  /**
   * Drop table MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl]
   */
  public static void dropMmaPartitionMeta(Connection conn, String db, String tbl) throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMD5(tbl));

    String ddl = "DROP TABLE " + schemaName + "." + tableName;
    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing DDL: {}", ddl);

      stmt.execute(ddl);
    }
  }

  /**
   * Return a record from MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl] if it exists, else null
   */
  public static MigrationJobPtInfo selectFromMmaPartitionMeta(Connection conn,
                                                              String uniqueId,
                                                              String jobType,
                                                              String db,
                                                              String tbl,
                                                              List<String> partitionValues)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMD5(tbl));
    String sql = String.format("SELECT * FROM %s.%s WHERE %s='%s' and %s='%s' and %s='%s'",
                               schemaName, tableName,
                               Constants.MMA_PT_META_COL_UNIQUE_ID, uniqueId,
                               Constants.MMA_PT_META_COL_JOB_TYPE, jobType,
                               Constants.MMA_PT_META_COL_PT_VALS,
                               GsonUtils.getFullConfigGson().toJson(partitionValues));

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if (!rs.next()) {
          return null;
        }
        return new MigrationJobPtInfo(partitionValues,
                                      MigrationStatus.valueOf(rs.getString(4)),
                                      rs.getInt(5),
                                      rs.getLong(6));
      }
    }
  }

  /**
   * Return records from MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl]
   */
  public static List<MigrationJobPtInfo> selectFromMmaPartitionMeta(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl,
      MigrationStatus status,
      int limit)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMD5(tbl));

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM ").append(schemaName).append(".").append(tableName);
    sb.append(String.format(" WHERE %s='%s' AND %s='%s'",
        Constants.MMA_PT_META_COL_UNIQUE_ID, uniqueId,
        Constants.MMA_PT_META_COL_JOB_TYPE, jobType));
    if (status != null) {
      sb.append(String.format(" AND %s='%s'",
                              Constants.MMA_PT_META_COL_STATUS,
                              status.toString()));
    }
    sb.append(" ORDER BY ").append(Constants.MMA_PT_META_COL_UNIQUE_ID).append(", ")
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
          MigrationJobPtInfo jobPtInfo =
              new MigrationJobPtInfo(
                  GsonUtils.getFullConfigGson().fromJson(rs.getString(3), type),
                  MigrationStatus.valueOf(rs.getString(4)),
                  rs.getInt(5),
                  rs.getLong(6));
          ret.add(jobPtInfo);
        }
        return ret;
      }
    }
  }

  public static Map<MigrationStatus, Integer> getPartitionStatusDistribution(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMD5(tbl));

    StringBuilder sb = new StringBuilder();
    sb
        .append("SELECT ")
        .append(Constants.MMA_PT_META_COL_STATUS).append(", COUNT(1) as CNT FROM ")
        .append(schemaName).append(".").append(tableName)
        .append(String.format(" WHERE %s='%s' AND %s='%s'",
            Constants.MMA_PT_META_COL_UNIQUE_ID, uniqueId,
            Constants.MMA_PT_META_COL_JOB_TYPE, jobType))
        .append(" GROUP BY ").append(Constants.MMA_PT_META_COL_STATUS);

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sb.toString());
      Map<MigrationStatus, Integer> ret = new HashMap<>();
      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        while (rs.next()) {
          MigrationStatus status =
              MigrationStatus.valueOf(rs.getString(1));
          Integer count = rs.getInt(2);
          ret.put(status, count);
        }

        return ret;
      }
    }
  }

  /**
   * Filter out existing partitions from candidates
   */
  public static List<List<String>> filterOutPartitions(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl,
      List<List<String>> candidates)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, getMD5(db));
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, getMD5(tbl));

    String sql = String.format("SELECT %s FROM %s.%s WHERE %s='%s' AND %s='%s' AND %s='%s'",
        Constants.MMA_PT_META_COL_PT_VALS,
        schemaName, tableName,
        Constants.MMA_PT_META_COL_UNIQUE_ID, uniqueId,
        Constants.MMA_PT_META_COL_JOB_TYPE, jobType,
        Constants.MMA_PT_META_COL_STATUS, MigrationStatus.SUCCEEDED.name());

    try (Statement stmt = conn.createStatement()) {
      LOG.debug("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        Set<String> managedPartitionValuesJsonSet = new HashSet<>();
        while (rs.next()) {
          managedPartitionValuesJsonSet.add(rs.getString(3));
        }

        Type type = new TypeToken<List<String>>() {}.getType();

        // Filter out existing partitions
        return candidates.stream()
            .map(ptv -> GsonUtils.getFullConfigGson().toJson(ptv))
            .filter(v -> !managedPartitionValuesJsonSet.contains(v))
            .map(json -> (List<String>) GsonUtils.getFullConfigGson().fromJson(json, type))
            .collect(Collectors.toList());
      }
    }
  }

  /**
   *  Infer a migration job's status from the statuses of its partitions
   */
  public static MigrationStatus inferPartitionedTableStatus(
      Connection conn,
      String uniqueId,
      String jobType,
      String db,
      String tbl)
      throws SQLException {

    Map<MigrationStatus, Integer> statusDistribution =
        MmaMetaManagerDbImplUtils.getPartitionStatusDistribution(conn, uniqueId, jobType, db, tbl);
    int total = statusDistribution.values().stream().reduce(0, Integer::sum);
    int pending =
        statusDistribution.getOrDefault(MigrationStatus.PENDING, 0);
    int succeeded =
        statusDistribution.getOrDefault(MigrationStatus.SUCCEEDED, 0);
    int failed =
        statusDistribution.getOrDefault(MigrationStatus.FAILED, 0);

    // Decide table status based on partition status
    if (total == succeeded) {
      return MigrationStatus.SUCCEEDED;
    } else if ((total == succeeded + failed) && failed != 0) {
      return MigrationStatus.FAILED;
    } else if ((total == pending + succeeded + failed) && pending != 0) {
      return MigrationStatus.PENDING;
    } else {
      return MigrationStatus.RUNNING;
    }
  }

  public static String generateMigrationUniqueId(String sourceDB,
                                                 String sourceTbl,
                                                 String destinationDB,
                                                 String destinationTbl) {
    if (StringUtils.isNullOrEmpty(sourceDB) ||
        StringUtils.isNullOrEmpty(sourceTbl) ||
        StringUtils.isNullOrEmpty(destinationDB) ||
        StringUtils.isNullOrEmpty(destinationTbl)) {
      LOG.error("Generate migration unique id failed, source db {}, source tb {}, destination db {}, destination tb {}",
          sourceDB, sourceTbl, destinationDB, destinationTbl);
      throw new RuntimeException("Generate migration unique id failed");
    }
    return sourceDB.toLowerCase() + "." + sourceTbl.toLowerCase() + ":"
        + destinationDB.toLowerCase() + "." + destinationTbl.toLowerCase();
  }

  public static String getMD5(String content) {
    try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      byte[] rawBit = md5.digest(content.getBytes());
      String outputMD5 = " ";
      for (int i = 0; i < 16; i++) {
        outputMD5 = outputMD5 + hexArray[rawBit[i] >>> 4 & 0x0f];
        outputMD5 = outputMD5 + hexArray[rawBit[i] & 0x0f];
      }
      String str = outputMD5.trim();
      LOG.info("Content: {}, MD5: {}", content, str);
      return str;
    } catch (NoSuchAlgorithmException e) {
      LOG.error("MD5 algorithm not found");
      return content;
    }
  }
}
