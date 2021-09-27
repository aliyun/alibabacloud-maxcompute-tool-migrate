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

package com.aliyun.odps.mma.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.aliyun.odps.mma.util.GsonUtils;

public abstract class AbstractConfiguration implements Map<String, String>, Configuration {

  /**
   * Java security configurations
   */
  public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
  public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
  public static final String JAVAX_SECURITY_AUTH_USESUBJECTCREDSONLY = "javax.security.auth.useSubjectCredsOnly";

  /**
   * Metadata source type. Could be Hive, MaxCompute, OSS.
   */
  public static final String METADATA_SOURCE_TYPE = "mma.metadata.source.type";

  /**
   * Metadata source credentials.
   */
  // OSS
  public static final String METADATA_SOURCE_OSS_ENDPOINT_EXTERNAL = "mma.metadata.source.oss.endpoint.external";
  public static final String METADATA_SOURCE_OSS_ENDPOINT_INTERNAL = "mma.metadata.source.oss.endpoint.internal";
  public static final String METADATA_SOURCE_OSS_BUCKET = "mma.metadata.source.oss.bucket";
  public static final String METADATA_SOURCE_OSS_PATH = "mma.metadata.source.oss.path";
  public static final String METADATA_SOURCE_OSS_ACCESS_KEY_ID = "mma.metadata.source.oss.access.key.id";
  public static final String METADATA_SOURCE_OSS_ACCESS_KEY_SECRET = "mma.metadata.source.oss.access.key.secret";
  public static final String METADATA_SOURCE_OSS_ROLE_ARN = "mma.metadata.source.oss.role.arn";
  // MaxCompute
  public static final String METADATA_SOURCE_MC_ENDPOINT = "mma.metadata.source.mc.endpoint";
  public static final String METADATA_SOURCE_MC_ACCESS_KEY_ID = "mma.metadata.source.mc.access.key.id";
  public static final String METADATA_SOURCE_MC_ACCESS_KEY_SECRET = "mma.metadata.source.mc.access.key.secret";
  // Hive
  public static final String METADATA_SOURCE_HIVE_IMPL = "mma.metadata.source.hive.impl";
  public static final String METADATA_SOURCE_HIVE_IMPL_DEFAULT_VALUE = "HMS";
  // Hive Meta store
  public static final String METADATA_SOURCE_HIVE_METASTORE_URIS = "mma.metadata.source.hive.metastore.uris";
  public static final String METADATA_SOURCE_HIVE_METASTORE_SASL_ENABLED = "mma.metadata.source.hive.metastore.sasl.enabled";
  public static final String METADATA_SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL = "mma.metadata.source.hive.metastore.kerberos.principal";
  public static final String METADATA_SOURCE_HIVE_METASTORE_KERBEROS_KEYTAB_FILE = "mma.metadata.source.hive.metastore.kerberos.keytab.file";
  public static final String METADATA_SOURCE_HIVE_META_STORE_EXTRA_CONFIGS = "mma.metadata.source.hive.metastore.extra.configs";
  // Hive JDBC
  public static final String METADATA_SOURCE_HIVE_JDBC_URL = "mma.metadata.source.hive.jdbc.url";
  public static final String METADATA_SOURCE_HIVE_JDBC_USERNAME = "mma.metadata.source.hive.jdbc.username";
  public static final String METADATA_SOURCE_HIVE_JDBC_PASSWORD = "mma.metadata.source.hive.jdbc.password";

  /**
   * Data source type. Could be Hive, MaxCompute, OSS.
   */
  public static final String DATA_SOURCE_TYPE = "mma.data.source.type";

  /**
   * Data source credentials.
   */
  // OSS
  public static final String DATA_SOURCE_OSS_ENDPOINT_EXTERNAL = "mma.data.source.oss.endpoint.external";
  public static final String DATA_SOURCE_OSS_ENDPOINT_INTERNAL = "mma.data.source.oss.endpoint.internal";
  public static final String DATA_SOURCE_OSS_BUCKET = "mma.data.source.oss.bucket";
  public static final String DATA_SOURCE_OSS_PATH = "mma.data.source.oss.path";
  public static final String DATA_SOURCE_OSS_ACCESS_KEY_ID = "mma.data.source.oss.access.key.id";
  public static final String DATA_SOURCE_OSS_ACCESS_KEY_SECRET = "mma.data.source.oss.access.key.secret";
  public static final String DATA_SOURCE_OSS_ROLE_ARN = "mma.data.source.oss.role.arn";
  // MaxCompute
  public static final String DATA_SOURCE_MC_ENDPOINT = "mma.data.source.mc.endpoint";
  public static final String DATA_SOURCE_MC_ACCESS_KEY_ID = "mma.data.source.mc.access.key.id";
  public static final String DATA_SOURCE_MC_ACCESS_KEY_SECRET = "mma.data.source.mc.access.key.secret";
  // Hive
  public static final String DATA_SOURCE_HIVE_JDBC_URL = "mma.data.source.hive.jdbc.url";
  public static final String DATA_SOURCE_HIVE_JDBC_USERNAME = "mma.data.source.hive.jdbc.username";
  public static final String DATA_SOURCE_HIVE_JDBC_PASSWORD = "mma.data.source.hive.jdbc.password";

  /**
   * Metadata destination type. Could be MaxCompute and OSS.
   */
  public static final String METADATA_DEST_TYPE = "mma.metadata.dest.type";

  /**
   * Metadata destination credentials.
   */
  // OSS
  public static final String METADATA_DEST_OSS_ENDPOINT_EXTERNAL = "mma.metadata.dest.oss.endpoint.external";
  public static final String METADATA_DEST_OSS_ENDPOINT_INTERNAL= "mma.metadata.dest.oss.endpoint.internal";
  public static final String METADATA_DEST_OSS_BUCKET = "mma.metadata.dest.oss.bucket";
  public static final String METADATA_DEST_OSS_ACCESS_KEY_ID = "mma.metadata.dest.oss.access.key.id";
  public static final String METADATA_DEST_OSS_ACCESS_KEY_SECRET = "mma.metadata.dest.oss.access.key.secret";
  public static final String METADATA_DEST_OSS_ROLE_ARN = "mma.metadata.dest.oss.role.arn";
  public static final String METADATA_DEST_OSS_PATH = "mma.metadata.dest.oss.path";
  // MaxCompute
  public static final String METADATA_DEST_MC_ENDPOINT = "mma.metadata.dest.mc.endpoint";
  public static final String METADATA_DEST_MC_ACCESS_KEY_ID = "mma.metadata.dest.mc.access.key.id";
  public static final String METADATA_DEST_MC_ACCESS_KEY_SECRET = "mma.metadata.dest.mc.access.key.secret";

  /**
   * Data destination type. Could be MaxCompute, OSS. Currently, the data destination type should
   * be the same as the metadata destination type.
   */
  public static final String DATA_DEST_TYPE = "mma.data.dest.type";

  /**
   * Data destination credentials.
   */
  // OSS
  public static final String DATA_DEST_OSS_ENDPOINT_EXTERNAL = "mma.data.dest.oss.endpoint.external";
  public static final String DATA_DEST_OSS_ENDPOINT_INTERNAL = "mma.data.dest.oss.endpoint.internal";
  public static final String DATA_DEST_OSS_BUCKET = "mma.data.dest.oss.bucket";
  public static final String DATA_DEST_OSS_ACCESS_KEY_ID = "mma.data.dest.oss.access.key.id";
  public static final String DATA_DEST_OSS_ACCESS_KEY_SECRET = "mma.data.dest.oss.access.key.secret";
  public static final String DATA_DEST_OSS_ROLE_ARN = "mma.data.dest.oss.role.arn";
  public static final String DATA_DEST_OSS_PATH = "mma.data.dest.oss.path";
  // MaxCompute
  public static final String DATA_DEST_MC_ENDPOINT = "mma.data.dest.mc.endpoint";
  public static final String DATA_DEST_MC_ACCESS_KEY_ID = "mma.data.dest.mc.access.key.id";
  public static final String DATA_DEST_MC_ACCESS_KEY_SECRET = "mma.data.dest.mc.access.key.secret";

  /**
   * Job attributes.
   */
  public static final String JOB_PRIORITY = "mma.job.priority";
  public static final String JOB_PRIORITY_DEFAULT_VALUE = "0";
  public static final String JOB_MAX_ATTEMPT_TIMES = "mma.job.max.attempt.times";
  public static final String JOB_MAX_ATTEMPT_TIMES_DEFAULT_VALUE = "3";
  public static final String JOB_EXECUTION_MC_PROJECT = "mma.job.execution.mc.project";
  public static final String JOB_NUM_DATA_WORKER = "mma.job.num.data.worker";
  public static final String JOB_NUM_DATA_WORKER_DEFAULT_VALUE = "5";

  /**
   * Knobs
   */
  public static final String TABLE_PARTITION_GROUP_SIZE = "mma.table.partition.group.size";
  public static final String TABLE_PARTITION_GROUP_SIZE_DEFAULT_VALUE = "50";
  public static final String TABLE_PARTITION_GROUP_SPLIT_SIZE = "mma.table.partition.group.split.size";
  public static final String TABLE_PARTITION_GROUP_SPLIT_SIZE_DEFAULT_VALUE = "5";

  Map<String, String> configuration;

  public AbstractConfiguration(Map<String, String> configuration) {
    Objects.requireNonNull(configuration);
    this.configuration = Collections.unmodifiableMap(new LinkedHashMap<>(configuration));
  }

  @Override
  public int size() {
    return configuration.size();
  }

  @Override
  public boolean isEmpty() {
    return configuration.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return configuration.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return configuration.containsValue(value);
  }

  @Override
  public String get(Object key) {
    return configuration.get(key);
  }

  @Override
  public String getOrDefault(Object key, String defaultValue) {
    return configuration.getOrDefault(key, defaultValue);
  }

  @Override
  public String put(String key, String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> keySet() {
    return new HashSet<>(configuration.keySet());
  }

  @Override
  public Collection<String> values() {
    return new ArrayList<>(configuration.values());
  }

  @Override
  public Set<Entry<String, String>> entrySet() {
    return new HashSet<>(configuration.entrySet());
  }

  @Override
  public String toString() {
    return GsonUtils.GSON.toJson(configuration);
  }
}
