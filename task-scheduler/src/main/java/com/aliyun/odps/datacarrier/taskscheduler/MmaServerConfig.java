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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MmaServerConfig {
  private static final Logger LOG = LogManager.getLogger(MmaServerConfig.class);

  private static final String ENV_VAR_MMA_META_DB_TYPE = "MMA_META_DB_TYPE";
  private static final String ENV_VAR_MMA_META_DB_USER = "MMA_META_DB_USER";
  private static final String ENV_VAR_MMA_META_DB_PASSWORD = "MMA_META_DB_PASSWORD";
  private static final String ENV_VAR_MMA_META_DB_JDBC_URL = "MMA_META_DB_JDBC_URL";

  private static final String ENV_VAR_MMA_API_SECURITY_ENABLED = "MMA_API_SECURITY_ENABLED";
  private static final String ENV_VAR_MMA_API_PRIVATE_KEY_PATH = "MMA_API_PRIVATE_KEY_PATH";
  private static final String ENV_VAR_MMA_API_PUBLIC_KEY_PATH = "MMA_API_PUBLIC_KEY_PATH";

  public static final String MMA_UI_ENABLED = "mma.ui.enabled";
  public static final String MMA_UI_ENABLED_DEFAULT_VALUE = "true";
  public static final String MMA_UI_HOST = "mma.ui.host";
  public static final String MMA_UI_HOST_DEFAULT_VALUE = "0.0.0.0";
  public static final String MMA_UI_PORT = "mma.ui.port";
  public static final String MMA_UI_PORT_DEFAULT_VALUE = "18888";
  public static final String MMA_UI_THREADS_MAX = "mma.ui.threads.max";
  public static final String MMA_UI_THREADS_MAX_DEFAULT_VALUE = "60";
  public static final String MMA_UI_THREADS_MIN = "mma.ui.threads.min";
  public static final String MMA_UI_THREADS_MIN_DEFAULT_VALUE = "10";

  public static final String MMA_API_ENABLED = "mma.api.enabled";
  public static final String MMA_API_ENABLED_DEFAULT_VALUE = "true";
  public static final String MMA_API_HOST = "mma.api.host";
  public static final String MMA_API_HOST_DEFAULT_VALUE = "0.0.0.0";
  public static final String MMA_API_PORT = "mma.api.port";
  public static final String MMA_API_PORT_DEFAULT_VALUE = "18889";
  public static final String MMA_API_THREADS_MAX = "mma.api.threads.max";
  public static final String MMA_API_THREADS_MAX_DEFAULT_VALUE = "60";
  public static final String MMA_API_THREADS_MIN = "mma.api.threads.min";
  public static final String MMA_API_THREADS_MIN_DEFAULT_VALUE = "10";
  public static final String MMA_API_SECURITY_ENABLED = "mma.api.security.enabled";
  public static final String MMA_API_SECURITY_ENABLED_DEFAULT_VALUE = "false";
  public static final String MMA_API_PRIVATE_KEY_PATH = "mma.api.security.privateKey.path";
  public static final String MMA_API_PUBLIC_KEY_PATH = "mma.api.security.publicKey.path";

  private static final Map<String, String> DEFAULT_API_CONFIG = new HashMap<>();
  private static final Map<String, String> DEFAULT_UI_CONFIG = new HashMap<>();

  static {
    DEFAULT_UI_CONFIG.put(
        MMA_UI_ENABLED,
        MMA_UI_ENABLED_DEFAULT_VALUE);
    DEFAULT_UI_CONFIG.put(
        MMA_UI_HOST,
        MMA_UI_HOST_DEFAULT_VALUE);
    DEFAULT_UI_CONFIG.put(
        MMA_UI_PORT,
        MMA_UI_PORT_DEFAULT_VALUE);
    DEFAULT_UI_CONFIG.put(
        MMA_UI_THREADS_MAX,
        MMA_UI_THREADS_MAX_DEFAULT_VALUE);
    DEFAULT_UI_CONFIG.put(
        MMA_UI_THREADS_MIN,
        MMA_UI_THREADS_MIN_DEFAULT_VALUE);

    DEFAULT_API_CONFIG.put(
        MMA_API_ENABLED,
        MMA_API_ENABLED_DEFAULT_VALUE);
    DEFAULT_API_CONFIG.put(
        MMA_API_SECURITY_ENABLED,
        MMA_API_SECURITY_ENABLED_DEFAULT_VALUE);
    DEFAULT_API_CONFIG.put(
        MMA_API_HOST,
        MMA_API_HOST_DEFAULT_VALUE);
    DEFAULT_API_CONFIG.put(
        MMA_API_PORT,
        MMA_API_PORT_DEFAULT_VALUE);
    DEFAULT_API_CONFIG.put(
        MMA_API_THREADS_MAX,
        MMA_API_THREADS_MAX_DEFAULT_VALUE);
    DEFAULT_API_CONFIG.put(
        MMA_API_THREADS_MIN,
        MMA_API_THREADS_MIN_DEFAULT_VALUE);
  }

  private static MmaServerConfig instance;
  private DataSource dataSource;
  private MmaConfig.OssConfig ossConfig;
  private MmaConfig.HiveConfig hiveConfig;
  private MmaConfig.OdpsConfig odpsConfig;
  private MmaConfig.MetaDbConfig metaDbConfig;
  private MmaEventConfig eventConfig;
  private Map<String, String> resourceConfig;
  private Map<String, String> uiConfig;
  private Map<String, String> apiConfig;
  private Map<String, String> hdfsConfig;

  MmaServerConfig() {
    this.dataSource = DataSource.Hive;
    this.ossConfig = null;
    this.hiveConfig = null;
    this.odpsConfig = null;
    this.metaDbConfig = new MmaConfig.MetaDbConfig();
    this.apiConfig = DEFAULT_API_CONFIG;
  }

  MmaServerConfig(
      DataSource dataSource,
      MmaConfig.OssConfig ossConfig,
      MmaConfig.HiveConfig hiveConfig,
      MmaConfig.OdpsConfig odpsConfig,
      MmaConfig.MetaDbConfig metaDbConfig,
      MmaEventConfig eventConfig,
      Map<String, String> resourceConfig,
      Map<String, String> uiConfig) {
    this.dataSource = dataSource;
    this.ossConfig = ossConfig;
    this.hiveConfig = hiveConfig;
    this.odpsConfig = odpsConfig;
    this.metaDbConfig = metaDbConfig;
    this.eventConfig = eventConfig;
    this.resourceConfig = resourceConfig;
    this.uiConfig = uiConfig;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public MmaConfig.OdpsConfig getOdpsConfig() {
    return odpsConfig;
  }

  public MmaConfig.HiveConfig getHiveConfig() {
    return hiveConfig;
  }

  public MmaConfig.MetaDbConfig getMetaDbConfig() {
    return metaDbConfig;
  }

  public void setMetaDbConfig(MmaConfig.MetaDbConfig metaDbConfig) {
    this.metaDbConfig = metaDbConfig;
  }

  public MmaConfig.OssConfig getOssConfig() {
    return ossConfig;
  }

  public MmaEventConfig getEventConfig() {
    return eventConfig;
  }

  public Map<String, String> getResourceConfig() {
    return resourceConfig;
  }

  public Map<String, String> getHdfsConfig() {
    return hdfsConfig;
  }

  public Map<String, String> getUIConfig() {
    if (uiConfig == null) {
      return DEFAULT_UI_CONFIG;
    }

    // Merge with default UI config, make sure necessary configurations exist
    Map<String, String> temp = new HashMap<>(DEFAULT_UI_CONFIG);
    temp.putAll(uiConfig);

    return temp;
  }

  public Map<String, String> getApiConfig() {
    if (apiConfig == null) {
      return DEFAULT_API_CONFIG;
    }

    // Merge with default API config, make sure necessary configurations exist
    Map<String, String> temp = new HashMap<>(DEFAULT_API_CONFIG);
    temp.putAll(apiConfig);

    return temp;
  }

  public String toJson() {
    return GsonUtils.getFullConfigGson().toJson(this);
  }

  public boolean validate() {
    boolean valid = true;

    if (dataSource == null) {
      valid = false;
      LOG.error("Validate MMA server config failed because datasource is not configured");
    }
    if (metaDbConfig == null) {
      valid = false;
      LOG.error("Validate MMA server config failed because MMA meta DB is not configured");
    } else if (!metaDbConfig.validate()) {
      valid = false;
      LOG.error("Validate MetaConfiguration failed due to {}", metaDbConfig);
    }

    return valid;
  }

  public static void init(Path path) throws IOException {
    instance = new MmaServerConfig();

    loadFromEnvVar();
    loadFromFile(path);

    if (!instance.validate()) {
      throw new IllegalArgumentException(
          "Invalid MmaServerConfig, see mma/log/mma_server.LOG for detailed reason");
    }
  }


  private static void loadFromFile(Path path) throws IOException {
    if (path == null || !path.toFile().exists()) {
      return;
    }

    String content = DirUtils.readFile(path);
    MmaServerConfig config = GsonUtils.getFullConfigGson().fromJson(content, MmaServerConfig.class);

    if (config.getDataSource() != null) {
      instance.dataSource = config.getDataSource();
    }
    if (config.getOssConfig() != null) {
      instance.ossConfig = config.getOssConfig();
    }
    if (config.getHiveConfig() != null) {
      instance.hiveConfig = config.getHiveConfig();
    }
    if (config.getOdpsConfig() != null) {
      instance.odpsConfig = config.getOdpsConfig();
    }
    if (config.getEventConfig() != null) {
      instance.eventConfig = config.getEventConfig();
    }
    if (config.getResourceConfig() != null) {
      instance.resourceConfig = config.getResourceConfig();
    }
    if (config.getUIConfig() != null) {
      instance.uiConfig = config.getUIConfig();
    }
    if (config.getApiConfig() != null) {
      instance.apiConfig.putAll(config.getApiConfig());
    }
    if (config.getHdfsConfig() != null) {
      instance.hdfsConfig = config.getHdfsConfig();
    }
    if (config.getMetaDbConfig() != null) {
      instance.metaDbConfig = config.getMetaDbConfig();
    }
  }

  private static void loadFromEnvVar() {
    String metaDbType = System.getenv(ENV_VAR_MMA_META_DB_TYPE);
    if (metaDbType != null) {
      instance.getMetaDbConfig().setDbType(metaDbType);
    }

    String metaDbUser = System.getenv(ENV_VAR_MMA_META_DB_USER);
    if (metaDbUser != null) {
      instance.getMetaDbConfig().setUser(metaDbUser);
    }

    String metaDbPassword = System.getenv(ENV_VAR_MMA_META_DB_PASSWORD);
    if (metaDbPassword != null) {
      instance.getMetaDbConfig().setPassword(metaDbPassword);
    }

    String metaDbJdbcUrl = System.getenv(ENV_VAR_MMA_META_DB_JDBC_URL);
    if (metaDbJdbcUrl != null) {
      instance.getMetaDbConfig().setJdbcUrl(metaDbJdbcUrl);
    }

    String mmaSecurityEnabled = System.getenv(ENV_VAR_MMA_API_SECURITY_ENABLED);
    if (mmaSecurityEnabled != null) {
      instance.apiConfig.put(MMA_API_SECURITY_ENABLED, mmaSecurityEnabled);
    }

    String mmaApiPublicKeyPath = System.getenv(ENV_VAR_MMA_API_PUBLIC_KEY_PATH);
    if (mmaApiPublicKeyPath != null) {
      instance.apiConfig.put(MMA_API_PUBLIC_KEY_PATH, mmaApiPublicKeyPath);
    }

    String mmaApiPrivateKeyPath = System.getenv(ENV_VAR_MMA_API_PRIVATE_KEY_PATH);
    if (mmaApiPrivateKeyPath != null) {
      instance.apiConfig.put(MMA_API_PRIVATE_KEY_PATH, mmaApiPrivateKeyPath);
    }
  }

  public static MmaServerConfig getInstance() {
    if (instance == null) {
      throw new IllegalStateException("MmaServerConfig not initialized");
    }

    return instance;
  }

  public static void setInstance(MmaServerConfig mmaServerConfig) {
    instance = Objects.requireNonNull(mmaServerConfig);
  }
}
