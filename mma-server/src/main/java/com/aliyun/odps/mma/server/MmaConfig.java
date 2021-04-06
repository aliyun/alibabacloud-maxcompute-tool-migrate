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

package com.aliyun.odps.mma.server;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.util.GsonUtils;
import com.aliyun.odps.utils.StringUtils;

// TODO: Split this class into several classes

public class MmaConfig {
  private static final Logger LOG = LogManager.getLogger(MmaConfig.class);

  public static interface Config
  {
    boolean validate();
  }

  public static class SQLSettingConfig {
    Map<String, String> ddlSettings = new HashMap<>();
    Map<String, String> migrationSettings = new HashMap<>();
    Map<String, String> verifySettings = new HashMap<>();

    transient boolean initialized = false;

    public SQLSettingConfig() {}

    public SQLSettingConfig(
        Map<String, String> ddlSettings,
        Map<String, String> migrationSettings,
        Map<String, String> verifySettings) {
      if (ddlSettings != null) {
        this.ddlSettings.putAll(ddlSettings);
      }
      if (migrationSettings != null) {
        this.migrationSettings.putAll(migrationSettings);
      }
      if (verifySettings != null) {
        this.verifySettings.putAll(verifySettings);
      }
    }

    public Map<String, String> getDDLSettings() {
      return ddlSettings;
    }

    public Map<String, String> getMigrationSettings() {
      return migrationSettings;
    }

    public Map<String, String> getVerifySettings() {
      return verifySettings;
    }

    public void initialize(Map<String, String> globalSettings) {
      for(Map.Entry<String, String> entry : globalSettings.entrySet()) {
        ddlSettings.putIfAbsent(entry.getKey(), entry.getValue());
        migrationSettings.putIfAbsent(entry.getKey(), entry.getValue());
        verifySettings.putIfAbsent(entry.getKey(), entry.getValue());
      }
      initialized = true;
    }

    public boolean isInitialized() {
      return this.initialized;
    }

    @Override
    public String toString() {
      return "{"
          + "ddlSettings=" + Objects.toString(ddlSettings, "null")
          + ", migrationSettings=" + Objects.toString(migrationSettings, "null")
          + ", verifySettings=" + Objects.toString(verifySettings, "null")
          + '}';
    }
  }

  public static class OssConfig implements Config {
    private String ossEndpoint;
    private String ossLocalEndpoint;
    private String ossBucket;
    private String ossRoleArn;
    private String ossAccessId;
    private String ossAccessKey;

    public OssConfig(String ossEndpoint, String ossLocalEndpoint, String ossBucket, String roleArn, String accessId, String accessKey) {
      this.ossEndpoint = ossEndpoint;
      this.ossLocalEndpoint = ossLocalEndpoint;
      this.ossBucket = ossBucket;
      this.ossRoleArn = roleArn;
      this.ossAccessId = accessId;
      this.ossAccessKey = accessKey;
    }

    @Override
    public boolean validate() {
      // TODO: try to connect
      if (StringUtils.isNullOrEmpty(ossEndpoint) ||
          StringUtils.isNullOrEmpty(ossLocalEndpoint) ||
          StringUtils.isNullOrEmpty(ossBucket)) {
        return false;
      }
      // arn, accessId and accessKey should not be empty at the same time
      if (StringUtils.isNullOrEmpty(ossRoleArn) &&
          StringUtils.isNullOrEmpty(ossAccessId) &&
          StringUtils.isNullOrEmpty(ossAccessKey)) {
        return false;
      }
      if (StringUtils.isNullOrEmpty(ossAccessId) != StringUtils.isNullOrEmpty(ossAccessKey)) {
        return false;
      }
      return true;
    }

    public String getOssEndpoint() {
      return ossEndpoint;
    }

    public String getOssLocalEndpoint() {
      return ossLocalEndpoint;
    }

    public String getOssBucket() {
      return ossBucket;
    }

    public String getOssRoleArn() {
      return ossRoleArn;
    }

    public String getOssAccessId() {
      return ossAccessId;
    }

    public String getOssAccessKey() {
      return ossAccessKey;
    }

    @Override
    public String toString() {
      return "OssDataSource {"
          + "ossEndpoint='" + Objects.toString(ossEndpoint, "null") + '\''
          + ", ossLocalEndpoint='" + Objects.toString(ossLocalEndpoint, "null") + '\''
          + ", ossBucket='" + Objects.toString(ossBucket, "null") + '\''
          + ", ossRoleArn='" + Objects.toString(ossRoleArn, "null") + '\''
          + '}';
    }
  }

  public static class HiveConfig implements Config {
    private String jdbcConnectionUrl;
    private String user;
    private String password;
    private String hmsThriftAddr;
    private String krbPrincipal;
    private String keyTab;
    private List<String> krbSystemProperties;
    private Map<String, String> globalSettings;
    private SQLSettingConfig sourceTableSettings;

    public HiveConfig(
        String jdbcConnectionUrl,
        String user,
        String password,
        String hmsThriftAddr,
        String krbPrincipal,
        String keyTab,
        List<String> krbSystemProperties,
        Map<String, String> globalSettings,
        SQLSettingConfig sourceTableSettings) {
      this.jdbcConnectionUrl = jdbcConnectionUrl;
      this.user = user;
      this.password = password;
      this.hmsThriftAddr = hmsThriftAddr;
      this.krbPrincipal = krbPrincipal;
      this.keyTab = keyTab;
      this.krbSystemProperties = krbSystemProperties;
      this.globalSettings = globalSettings;
      this.sourceTableSettings = sourceTableSettings;
    }

    @Override
    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(jdbcConnectionUrl) &&
              !StringUtils.isNullOrEmpty(hmsThriftAddr) &&
              user != null &&
              password != null);
    }

    public String getJdbcConnectionUrl() {
      return jdbcConnectionUrl;
    }

    public String getUser() {
      return user;
    }

    public String getPassword() {
      return password;
    }

    public String getHmsThriftAddr() {
      return hmsThriftAddr;
    }

    public String getKrbPrincipal() {
      return krbPrincipal;
    }

    public String getKeyTab() {
      return keyTab;
    }

    public List<String> getKrbSystemProperties() {
      return krbSystemProperties;
    }

    public SQLSettingConfig getSourceTableSettings() {
      if (sourceTableSettings == null) {
        sourceTableSettings = new SQLSettingConfig();
      }
      if (!sourceTableSettings.isInitialized()) {
        sourceTableSettings.initialize(
            globalSettings == null ? MapUtils.EMPTY_MAP : globalSettings);
      }
      return sourceTableSettings;
    }

    @Override
    public String toString() {
      return "HiveConfig {" + "hiveJdbcAddress='"
          + Objects.toString(jdbcConnectionUrl, "null") + '\''
          + ", hmsThriftAddr='"
          + Objects.toString(hmsThriftAddr, "null") + '\''
          + ", krbPrincipal='"
          + Objects.toString(krbPrincipal, "null") + '\''
          + ", keyTab='" + Objects.toString(keyTab, "null") + '\''
          + ", krbSystemProperties="
          + Objects.toString(krbSystemProperties, "null")
          + ", sourceTableSettings=" + Objects.toString(sourceTableSettings, "null")
          + '}';
    }
  }

  public static class OdpsConfig implements Config {
    private String accessId;
    private String accessKey;
    private String endpoint;
    private String projectName;
    private Map<String, String> globalSettings;
    private SQLSettingConfig sourceTableSettings;
    private SQLSettingConfig destinationTableSettings;

    public OdpsConfig(String accessId, String accessKey, String endpoint, String projectName) {
      this.accessId = accessId;
      this.accessKey = accessKey;
      this.endpoint = endpoint;
      this.projectName = projectName;
    }

    public String getAccessId() {
      return accessId;
    }

    public String getAccessKey() {
      return accessKey;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public String getProjectName() {
      return projectName;
    }

    public Map<String, String> getGlobalSettings() {
      return globalSettings == null ? MapUtils.EMPTY_MAP : globalSettings;
    }

    public SQLSettingConfig getSourceTableSettings() {
      if (sourceTableSettings == null) {
        sourceTableSettings = new SQLSettingConfig();
      }
      if (!sourceTableSettings.isInitialized()) {
        sourceTableSettings.initialize(
            globalSettings == null ? MapUtils.EMPTY_MAP : globalSettings);
      }
      return sourceTableSettings;
    }

    public SQLSettingConfig getDestinationTableSettings() {
      if (destinationTableSettings == null) {
        destinationTableSettings = new SQLSettingConfig();
      }
      if (!destinationTableSettings.isInitialized()) {
        destinationTableSettings.initialize(
            globalSettings == null ? MapUtils.EMPTY_MAP : globalSettings);
      }
      return destinationTableSettings;
    }

    public Odps toOdps() {
      AliyunAccount aliyunAccount = new AliyunAccount(accessId, accessKey);
      Odps odps = new Odps(aliyunAccount);
      odps.setDefaultProject(this.projectName);
      odps.setEndpoint(this.endpoint);

      return odps;
    }

    @Override
    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(accessId) &&
              !StringUtils.isNullOrEmpty(accessKey) &&
              !StringUtils.isNullOrEmpty(endpoint) &&
              !StringUtils.isNullOrEmpty(projectName));
    }

    @Override
    public String toString() {
      return "OdpsConfig {"
          + "accessId='" + Objects.toString(accessId, "null") + '\''
          + ", accessKey='" + Objects.toString(accessKey, "null") + '\''
          + ", endpoint='" + Objects.toString(endpoint, "null") + '\''
          + ", projectName='" + Objects.toString(projectName, "null") + '\''
          + ", globalSettings=" + Objects.toString(globalSettings, "null")
          + ", sourceTableSettings=" + Objects.toString(sourceTableSettings, "null")
          + ", destinationTableSettings=" + Objects.toString(destinationTableSettings, "null")
          + '}';
    }
  }

  public static class MetaDbConfig implements Config {
    private String dbType;
    private String jdbcUrl;
    private String user;
    private String password;
    private int maxPoolSize;

    public MetaDbConfig(boolean onDataworks) {
      dbType = "h2";
      if (onDataworks) {
        jdbcUrl = "jdbc:h2:mem:MMA;DB_CLOSE_DELAY=-1";
      } else {
        String mmaHome = System.getenv("MMA_HOME");
        Path parentDir = Paths.get(mmaHome);
        jdbcUrl = "jdbc:h2:file:" + Paths.get(parentDir.toString(), ".MmaMeta").toAbsolutePath()
            + ";AUTO_SERVER=TRUE";
      }
      user = "mma";
      password = "mma";
      maxPoolSize = 50;
    }

    public MetaDbConfig(
        String dbType,
        String jdbcUrl,
        String user,
        String password,
        int maxPoolSize) {
      this.dbType = dbType.toLowerCase();
      this.jdbcUrl = jdbcUrl;
      this.user = user;
      this.password = password;
      this.maxPoolSize = maxPoolSize;
    }

    public String getDbType() {
      return dbType;
    }

    public void setDbType(String dbType) {
      this.dbType = dbType;
    }

    public String getDriverClass() {
      if ("mysql".equals(dbType)) {
        return "com.mysql.cj.jdbc.Driver";
      } else if ("h2".equals(dbType)) {
        return "org.h2.Driver";
      }
      throw new RuntimeException("Unknown meta db type: " + dbType);
    }

    public String getJdbcUrl() {
      return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public int getMaxPoolSize() {
      return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
    }

    @Override
    public boolean validate() {
      if (StringUtils.isNullOrEmpty(dbType) ||
          StringUtils.isNullOrEmpty(jdbcUrl) ||
          StringUtils.isNullOrEmpty(user) ||
          StringUtils.isNullOrEmpty(password) || maxPoolSize <= 0) {
        LOG.error("Required fields 'dbType', 'jdbcUrl', 'user', 'password', 'maxPoolSize' is null or invalid value");
        return false;
      }

      try {
        Class.forName(getDriverClass());
      } catch (ClassNotFoundException e) {
        LOG.error("Driver class not found", getDriverClass());
        return false;
      }

      try {
        DriverManager.getConnection(jdbcUrl, user, password);
      } catch (SQLException e) {
        LOG.error("Failed to create connection", e);
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return GsonUtils.GSON.toJson(this);
    }
  }

  public static class ServiceMigrationConfig implements Config {
    private String destProjectName;

    public ServiceMigrationConfig (String destProjectName) {
      this.destProjectName = destProjectName;
    }

    public String getDestProjectName() {
      return destProjectName;
    }

    @Override
    public boolean validate() {
      return !StringUtils.isNullOrEmpty(destProjectName);
    }
  }

  public static class DatabaseMigrationConfig implements Config {
    private String sourceDatabaseName;
    private String destProjectName;
    private String destProjectStorage;
    private AdditionalTableConfig additionalTableConfig;

    public DatabaseMigrationConfig (
        String sourceDatabaseName,
        String destProjectName,
        AdditionalTableConfig additionalTableConfig) {
      this.sourceDatabaseName = sourceDatabaseName;
      this.destProjectName = destProjectName;
      this.destProjectStorage = null;
      this.additionalTableConfig = additionalTableConfig;
    }

    public String getSourceDatabaseName() {
      return sourceDatabaseName;
    }

    public String getDestProjectName() {
      return destProjectName;
    }

    public String getDestProjectStorage() {
      return destProjectStorage;
    }

    public AdditionalTableConfig getAdditionalTableConfig() {
      return additionalTableConfig;
    }

    @Override
    public boolean validate() {
      return !StringUtils.isNullOrEmpty(sourceDatabaseName)
             && !StringUtils.isNullOrEmpty(destProjectName)
             && (additionalTableConfig == null || additionalTableConfig.validate());
    }
  }

  public static class DatabaseBackupConfig implements Config {

    private String databaseName;
    private List<ObjectType> objectTypes;
    private String backupName;
    private AdditionalTableConfig additionalTableConfig;

    DatabaseBackupConfig(
        String databaseName,
        List<ObjectType> objectTypes,
        String backupName) {
      this.databaseName = databaseName;
      this.objectTypes = objectTypes;
      this.backupName = backupName;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public List<ObjectType> getObjectTypes() {
      return objectTypes;
    }

    public String getBackupName() {
      return backupName;
    }

    public AdditionalTableConfig getAdditionalTableConfig() {
      return additionalTableConfig;
    }

    public void setAdditionalTableConfig(AdditionalTableConfig additionalTableConfig) {
      this.additionalTableConfig = additionalTableConfig;
    }

    @Override
    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(databaseName)
          && !StringUtils.isNullOrEmpty(backupName)
          && objectTypes != null);
    }
  }

  public static class DatabaseRestoreConfig implements Config {
    private String sourceDatabaseName;
    private String destinationDatabaseName;
    private List<ObjectType> objectTypes;
    private String backupName;
    private boolean update = true;
    private AdditionalTableConfig additionalTableConfig;
    private Map<String, String> settings;
    private OdpsConfig odpsConfig;
    private OssConfig ossConfig;

    public DatabaseRestoreConfig(
        String sourceDatabaseName,
        String destinationDatabaseName,
        List<ObjectType> types,
        boolean update,
        String backupName,
        Map<String, String> settings) {
      this.sourceDatabaseName = sourceDatabaseName;
      this.destinationDatabaseName = destinationDatabaseName;
      this.objectTypes = types;
      this.backupName = backupName;
      this.update = update;
      this.settings = settings;
    }

    public String getSourceDatabaseName() {
      return sourceDatabaseName;
    }

    public String getDestinationDatabaseName() {
      return destinationDatabaseName;
    }

    public List<ObjectType> getObjectTypes() {
      return objectTypes;
    }

    public String getBackupName() {
      return backupName;
    }

    public boolean isUpdate() {
      return update;
    }

    public AdditionalTableConfig getAdditionalTableConfig() {
      return additionalTableConfig;
    }

    public Map<String, String> getSettings() {
      return settings;
    }

    public OdpsConfig getOdpsConfig() {
      return odpsConfig;
    }

    public OssConfig getOssConfig() {
      return ossConfig;
    }

    public void setOdpsConfig(OdpsConfig odpsConfig) {
      this.odpsConfig = odpsConfig;
    }

    public void setOssConfig(OssConfig ossConfig) {
      this.ossConfig = ossConfig;
    }

    public void setAdditionalTableConfig(AdditionalTableConfig additionalTableConfig) {
      this.additionalTableConfig = additionalTableConfig;
    }

    public static DatabaseRestoreConfig fromJson(String json) {
      return GsonUtils.GSON.fromJson(json, DatabaseRestoreConfig.class);
    }

    public static String toJson(DatabaseRestoreConfig config) {
      return GsonUtils.GSON.toJson(config);
    }

    @Override
    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(sourceDatabaseName)
          && !StringUtils.isNullOrEmpty(destinationDatabaseName)
          && !StringUtils.isNullOrEmpty(backupName)
          && objectTypes != null);
    }
  }

  public static class TableMigrationConfig implements Config {
    private DataSource dataSource;
    private String sourceDatabaseName;
    private String sourceTableName;
    private String destProjectName;
    private String destTableName;
    private String destTableStorage;
    private List<List<String>> partitionValuesList;
    private List<String> beginPartition;
    private List<String> endPartition;
    private AdditionalTableConfig additionalTableConfig;
    private HiveConfig hiveConfig;
    private OdpsConfig odpsConfig;

    public TableMigrationConfig(
        DataSource dataSource,
        String sourceDataBaseName,
        String sourceTableName,
        String destProjectName,
        String destTableName,
        AdditionalTableConfig additionalTableConfig) {
      this(
          dataSource,
          sourceDataBaseName,
          sourceTableName,
          destProjectName,
          destTableName,
          null,
          null,
          additionalTableConfig);
    }

    public TableMigrationConfig(
        DataSource dataSource,
        String sourceDatabaseName,
        String sourceTableName,
        String destProjectName,
        String destTableName,
        String destTableStorage,
        List<List<String>> partitionValuesList,
        AdditionalTableConfig additionalTableConfig) {
      this.dataSource = dataSource;
      this.sourceDatabaseName = sourceDatabaseName;
      this.sourceTableName = sourceTableName;
      this.destProjectName = destProjectName;
      this.destTableName = destTableName;
      this.destTableStorage = destTableStorage;
      this.partitionValuesList = partitionValuesList;
      this.additionalTableConfig = additionalTableConfig;
    }

    public String getSourceDataBaseName() {
      return sourceDatabaseName;
    }

    public String getSourceTableName() {
      return sourceTableName;
    }

    public String getDestProjectName() {
      return destProjectName;
    }

    public String getDestTableName() {
      return destTableName;
    }

    public String getDestTableStorage() {
      return destTableStorage;
    }

    public void setDestTableStorage(String storage) {
      this.destTableStorage = storage;
    }

    public List<List<String>> getPartitionValuesList() {
      return partitionValuesList;
    }

    public List<String> getBeginPartition() {
      return beginPartition;
    }

    public List<String> getEndPartition() {
      return endPartition;
    }

    public AdditionalTableConfig getAdditionalTableConfig() {
      return additionalTableConfig;
    }

    public void addPartitionValues(List<String> partitionValues) {
      if (partitionValuesList == null) {
        partitionValuesList = new LinkedList<>();
      }

      if (!partitionValuesList.contains(partitionValues)) {
        partitionValuesList.add(partitionValues);
      }
    }


    public void setAdditionalTableConfig(AdditionalTableConfig additionalTableConfig) {
      this.additionalTableConfig = additionalTableConfig;
    }

    public void setHiveConfig(HiveConfig hiveConfig) {
      this.hiveConfig = hiveConfig;
    }

//    public void setHdfsConfig(HdfsConfig hdfsConfig) { this.hdfsConfig = hdfsConfig; }

    public void setOdpsConfig(OdpsConfig odpsConfig) {
      this.odpsConfig = odpsConfig;
    }

    public HiveConfig getHiveConfig() {
      return hiveConfig;
    }

//    public HdfsConfig getHdfsConfig() { return this.hdfsConfig; }

    public OdpsConfig getOdpsConfig() {
      return this.odpsConfig;
    }

    public void apply(TableMetaModel tableMetaModel) {
//      TypeTransformer typeTransformer;
//      tableMetaModel.odpsProjectName =
//          (this.destProjectName == null) ? null : this.destProjectName.toLowerCase();
//      tableMetaModel.odpsTableName =
//          (this.destTableName == null) ? null : this.destTableName.toLowerCase();
//      tableMetaModel.odpsTableStorage = this.destTableStorage;
//
//      if (DataSource.Hive.equals(this.dataSource)) {
//        typeTransformer = new HiveTypeTransformer();
//      } else if (DataSource.ODPS.equals(this.dataSource)) {
//        typeTransformer = new McTypeTransformer(!StringUtils.isNullOrEmpty(destTableStorage));
//      } else {
//        throw new IllegalArgumentException("Unsupported datasource type: " + dataSource.name());
//      }
//
//      for (MetaSource.ColumnMetaModel c : tableMetaModel.columns) {
//        c.odpsColumnName = c.columnName;
//        c.odpsType = typeTransformer.toMcTypeV2(c.type).getTransformedType();
//      }
//
//      for (MetaSource.ColumnMetaModel pc : tableMetaModel.partitionColumns) {
//        pc.odpsColumnName = pc.columnName;
//        pc.odpsType = typeTransformer.toMcTypeV2(pc.type).getTransformedType();
//      }
//
//      // TODO: make it a general config
//      for (MetaSource.ColumnMetaModel pc : tableMetaModel.partitionColumns) {
//        if ("DATE".equalsIgnoreCase(pc.type)) {
//          pc.odpsType = "STRING";
//        }
//      }
    }

    public static TableMigrationConfig fromJson(String json) {
      return GsonUtils.GSON.fromJson(json, TableMigrationConfig.class);
    }

    public static String toJson(TableMigrationConfig config) {
      return GsonUtils.GSON.toJson(config);
    }

    @Override
    public boolean validate() {
      if (StringUtils.isNullOrEmpty(sourceDatabaseName)
          || StringUtils.isNullOrEmpty(destProjectName)
          // when backup a database, sourceTableName and destTableName will be empty
          || StringUtils.isNullOrEmpty(sourceTableName) != StringUtils.isNullOrEmpty(destTableName)
          || (partitionValuesList != null && partitionValuesList.stream().anyMatch(List::isEmpty))
          || (additionalTableConfig != null && !additionalTableConfig.validate())) {
        return false;
      }
      if (StringUtils.isNullOrEmpty(sourceTableName)
          && StringUtils.isNullOrEmpty(destTableStorage)) {
        return false;
      }
      return true;
    }
  }

  public enum ObjectType {
    TABLE,
    VIEW,
    RESOURCE,
    FUNCTION,
    DATABASE
  }

  public static class ObjectBackupConfig extends TableMigrationConfig {
    private String databaseName;
    private String objectName;
    private ObjectType objectType;
    private String backupName;
    private OssConfig ossConfig;

    public ObjectBackupConfig(
        DataSource dataSource,
        String databaseName,
        String objectName,
        ObjectType type,
        String backupName,
        AdditionalTableConfig additionalTableConfig) {
      super(
          dataSource,
          databaseName,
          objectName,
          databaseName,
          objectName + "_" + backupName,
          additionalTableConfig);
      this.databaseName = databaseName;
      this.objectName = objectName;
      this.objectType = type;
      this.backupName = backupName;
    }

    public String getDatabaseName() {
      return databaseName;
    }

    public String getObjectName() {
      return objectName;
    }

    public ObjectType getObjectType() {
      return objectType;
    }

    public String getBackupName() {
      return this.backupName;
    }

    public OssConfig getOssConfig() {
      return this.ossConfig;
    }

    public void setOssConfig(OssConfig ossConfig) {
      this.ossConfig = ossConfig;
    }

    public static ObjectBackupConfig fromJson(String json) {
      return GsonUtils.GSON.fromJson(json, ObjectBackupConfig.class);
    }

    public static String toJson(ObjectBackupConfig config) {
      return GsonUtils.GSON.toJson(config);
    }
  }

  public static class ObjectRestoreConfig {
    private String sourceDatabaseName;
    private String destinationDatabaseName;
    private String objectName;
    private ObjectType objectType;
    private boolean update;
    private String backupName;
    private Map<String, String> settings;
    private OdpsConfig odpsConfig;
    private OssConfig ossConfig;

    public ObjectRestoreConfig(
        String sourceDatabaseName,
        String destinationDatabaseName,
        String objectName,
        ObjectType objectType,
        boolean update,
        String backupName,
        Map<String, String> settings) {

      this.sourceDatabaseName = sourceDatabaseName;
      this.destinationDatabaseName = destinationDatabaseName;
      this.objectName = objectName;
      this.objectType = objectType;
      this.update = update;
      this.backupName = backupName;
      this.settings = settings;
    }

    public String getSourceDatabaseName() {
      return sourceDatabaseName;
    }

    public String getDestinationDatabaseName() {
      return destinationDatabaseName;
    }

    public String getObjectName() {
      return objectName;
    }

    public ObjectType getObjectType() {
      return objectType;
    }

    public boolean isUpdate() {
      return update;
    }

    public String getBackupName() {
      return backupName;
    }

    public Map<String, String> getSettings() {
      return settings;
    }

    public OdpsConfig getOdpsConfig() {
      return odpsConfig;
    }

    public void setOdpsConfig(OdpsConfig odpsConfig) {
      this.odpsConfig = odpsConfig;
    }

    public OssConfig getOssConfig() {
      return ossConfig;
    }

    public void setOssConfig(OssConfig ossConfig) {
      this.ossConfig = ossConfig;
    }

    public static ObjectRestoreConfig fromJson(String json) {
      return GsonUtils.GSON.fromJson(json, ObjectRestoreConfig.class);
    }

    public static String toJson(ObjectRestoreConfig config) {
      return GsonUtils.GSON.toJson(config);
    }
  }

  public static class AdditionalTableConfig implements Config {
    private int partitionGroupSize;
    private int retryTimesLimit;
    private int partitionGroupSplitSizeInGb = Constants.DEFAULT_PARTITION_GROUP_SPLIT_SIZE_IN_GB;

    public AdditionalTableConfig(int partitionGroupSize, int retryTimesLimit) {
      this.partitionGroupSize = partitionGroupSize;
      this.retryTimesLimit = retryTimesLimit;
    }

    public int getPartitionGroupSize() {
      return partitionGroupSize;
    }

    public int getRetryTimesLimit() {
      return retryTimesLimit;
    }

    public int getPartitionGroupSplitSizeInGb() {
      return partitionGroupSplitSizeInGb;
    }

    @Override
    public boolean validate() {
      if (retryTimesLimit < 0) {
        return false;
      }
      if (partitionGroupSize <= 0) {
        return false;
      }
      return true;
    }
  }

  public enum JobType {
    MIGRATION,
    BACKUP,
    RESTORE
  }

  /**
   * Used to record job description and transfer from MmaClient to MmaServer
   */
  public static class JobConfig {
    private String description;
    private AdditionalTableConfig additionalTableConfig;

    public JobConfig(String desc, AdditionalTableConfig additionalTableConfig) {
      this.description = desc;
      this.additionalTableConfig = additionalTableConfig;
    }

    public String getDescription() {
      return description;
    }

    public AdditionalTableConfig getAdditionalTableConfig() {
      return additionalTableConfig;
    }
  }
}
