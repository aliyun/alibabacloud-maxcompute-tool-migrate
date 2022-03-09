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

package com.aliyun.odps.mma.server.config;

import java.util.Collections;
import java.util.Map;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.MetaSourceType;

public class MmaServerConfiguration extends AbstractConfiguration {

  /**
   * Deploy mode. Could be local or dataworks.
   */
  public static final String DEPLOY_MODE = "mma.deploy.mode";
  public static final String DEPLOY_MODE_DEFAULT_VALUE = "local";

  /**
   * Database configurations. Also used by mma/bin/configure.
   */
  public static final String META_DB_TYPE = "mma.meta.db.type";
  public static final String META_DB_JDBC_URL = "mma.meta.db.jdbc.url";
  public static final String META_DB_JDBC_USERNAME = "mma.meta.db.jdbc.username";
  public static final String META_DB_JDBC_PASSWORD = "mma.meta.db.jdbc.password";

  /**
   * UI configurations
   */
  public static final String UI_ENABLED = "mma.ui.enabled";
  public static final String UI_ENABLED_DEFAULT_VALUE = "true";
  public static final String UI_HOST = "mma.ui.host";
  public static final String UI_HOST_DEFAULT_VALUE = "0.0.0.0";
  public static final String UI_PORT = "mma.ui.port";
  public static final String UI_PORT_DEFAULT_VALUE = "18888";
  public static final String UI_THREADS_MAX = "mma.ui.threads.max";
  public static final String UI_THREADS_MAX_DEFAULT_VALUE = "60";
  public static final String UI_THREADS_MIN = "mma.ui.threads.min";
  public static final String UI_THREADS_MIN_DEFAULT_VALUE = "10";

  /**
   * API configurations
   */
  public static final String API_ENABLED = "mma.api.enabled";
  public static final String API_ENABLED_DEFAULT_VALUE = "true";
  public static final String API_HOST = "mma.api.host";
  public static final String API_HOST_DEFAULT_VALUE = "0.0.0.0";
  public static final String API_PORT = "mma.api.port";
  public static final String API_PORT_DEFAULT_VALUE = "18889";
  public static final String API_THREADS_MAX = "mma.api.threads.max";
  public static final String API_THREADS_MAX_DEFAULT_VALUE = "60";
  public static final String API_THREADS_MIN = "mma.api.threads.min";
  public static final String API_THREADS_MIN_DEFAULT_VALUE = "10";
  public static final String API_SECURITY_ENABLED = "mma.api.security.enabled";
  public static final String API_SECURITY_ENABLED_DEFAULT_VALUE = "false";
  public static final String API_PRIVATE_KEY_PATH = "mma.api.security.privateKey.path";
  public static final String API_PUBLIC_KEY_PATH = "mma.api.security.publicKey.path";

  /**
   * Event configurations
   */
  public static final String EVENT_ENABLED = "mma.event.enabled";
  public static final String EVENT_ENABLED_DEFAULT_VALUE = "false";
  public static final String EVENT_SENDERS = "mma.event.senders";
  public static final String EVENT_TYPES = "mma.event.types";
  public static final String EVENT_TYPES_DEFAULT_VALUE = "SUMMARY";

  /**
   * Resource configurations
   */
  public static final String RESOURCE_DATA_WORKER = "mma.resource.data.worker";
  public static final String RESOURCE_DATA_WORKER_DEFAULT_VALUE = "25";
  public static final String RESOURCE_METADATA_WORKER = "mma.resource.metadata.worker";
  public static final String RESOURCE_METADATA_WORKER_DEFAULT_VALUE = "5";

  private static MmaServerConfiguration instance;

  private MmaServerConfiguration(Map<String, String> builder) {
    super(builder);
    validate();
  }

  private static void setUpDefaultConfiguration(Map<String, String> builder) {
    boolean sourceIsHiveAndConnectorPathNotSet =
        MetaSourceType.Hive.name().equals(builder.get(METADATA_SOURCE_TYPE)) &&
        !builder.containsKey(METADATA_SOURCE_CONNECTOR_PATH);
    if (sourceIsHiveAndConnectorPathNotSet) {
      String defaultHiveJar = "/lib/connector/hive-uber.jar";
      builder.put(METADATA_SOURCE_CONNECTOR_PATH,
          MetaSourceType.Hive.name() + ":" + System.getenv("MMA_HOME") + defaultHiveJar);
    }
  }

  @Override
  public void validate() {
    // TODO:
  }

  public synchronized static MmaServerConfiguration getInstance() {
    if (instance == null) {
      instance = new MmaServerConfiguration(Collections.emptyMap());
    }

    return instance;
  }

  public synchronized static void setInstance(Map<String, String> builder) {
    setUpDefaultConfiguration(builder);
    instance = new MmaServerConfiguration(builder);
  }
}
