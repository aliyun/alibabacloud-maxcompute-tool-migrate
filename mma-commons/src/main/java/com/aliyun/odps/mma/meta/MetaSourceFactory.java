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

package com.aliyun.odps.mma.meta;


import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class MetaSourceFactory {

  public MetaSourceFactory() {}

  public synchronized MetaSource getMetaSource(AbstractConfiguration config) throws Exception {
    String metadataSourceType = config.get(AbstractConfiguration.METADATA_SOURCE_TYPE);
    switch (metadataSourceType) {
      case "MaxCompute": {
        return new McMetaSource(
            config.get(AbstractConfiguration.METADATA_SOURCE_MC_ACCESS_KEY_ID),
            config.get(AbstractConfiguration.METADATA_SOURCE_MC_ACCESS_KEY_SECRET),
            config.get(AbstractConfiguration.METADATA_SOURCE_MC_ENDPOINT));
      }
      case "OSS": {
        String ossEndpointForMma = config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_INTERNAL);
        if (config.containsKey(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_EXTERNAL)) {
          ossEndpointForMma = config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_EXTERNAL);
        }
        return new OssMetaSource(
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_ID),
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_SECRET),
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_BUCKET),
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_PATH),
            ossEndpointForMma);
      }
      case "Hive": {
        return HiveMetaSourceFactory.getHiveMetaSource(config);
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata source type: " + metadataSourceType);
    }
  }

  static class HiveMetaSourceFactory {
    private static final int MAX_HIVE_META_SOURCE_NUM = 5;
    private static final Deque<HiveMetaSourceInstance> RUNNING_HIVE_QUEUE = new LinkedList<>();

    static MetaSource getHiveMetaSource(AbstractConfiguration config) throws Exception {
      HiveMetaSourceInstance newInstance = new HiveMetaSourceInstance(config);
      for (HiveMetaSourceInstance instance: RUNNING_HIVE_QUEUE) {
        if (instance.equals(newInstance)){
          RUNNING_HIVE_QUEUE.remove(instance);
          RUNNING_HIVE_QUEUE.addFirst(instance);
          return instance.getMetaSource();
        }
      }
      if (RUNNING_HIVE_QUEUE.size() >= MAX_HIVE_META_SOURCE_NUM) {
        RUNNING_HIVE_QUEUE.pollLast();
      }
      RUNNING_HIVE_QUEUE.addFirst(newInstance);
      return newInstance.getMetaSource();
    }

  }

  static class HiveMetaSourceInstance {

    private final AbstractConfiguration config;
    private MetaSource metaSource;
    private boolean isHms;

    HiveMetaSourceInstance(AbstractConfiguration config) {
      this.config = config;
      setUpType();
    }

    MetaSource getMetaSource() throws MmaException {
      if (metaSource != null) {
        return metaSource;
      }

      Map<String, String> javaSecurityConfigs = new HashMap<>();
      if (config.containsKey(AbstractConfiguration.JAVA_SECURITY_AUTH_LOGIN_CONFIG)) {
        javaSecurityConfigs.put(
            AbstractConfiguration.JAVA_SECURITY_AUTH_LOGIN_CONFIG,
            config.get(AbstractConfiguration.JAVA_SECURITY_AUTH_LOGIN_CONFIG));
      }
      if (config.containsKey(AbstractConfiguration.JAVA_SECURITY_KRB5_CONF)) {
        javaSecurityConfigs.put(
            AbstractConfiguration.JAVA_SECURITY_KRB5_CONF,
            config.get(AbstractConfiguration.JAVA_SECURITY_KRB5_CONF));
      }
      if (config.containsKey(AbstractConfiguration.JAVAX_SECURITY_AUTH_USESUBJECTCREDSONLY)) {
        javaSecurityConfigs.put(
            AbstractConfiguration.JAVAX_SECURITY_AUTH_USESUBJECTCREDSONLY,
            config.get(AbstractConfiguration.JAVAX_SECURITY_AUTH_USESUBJECTCREDSONLY));
      }

      MetaConfig metaConfig = null;
      if (isHms) {
        Map<String, String> extraConfigs;
        String extraConfigsStr = config.get(
            AbstractConfiguration.METADATA_SOURCE_HIVE_META_STORE_EXTRA_CONFIGS);
        if (!StringUtils.isBlank(extraConfigsStr)) {
          extraConfigs = GsonUtils
              .GSON
              .fromJson(extraConfigsStr, new TypeToken<Map<String, String>>() {}.getType());
        } else {
          extraConfigs = new HashMap<>();
        }

        metaConfig = new HiveMetaConfig(
            config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_URIS),
            Boolean.parseBoolean(config.get(
                AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_SASL_ENABLED)),
            config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL),
            config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_KEYTAB_FILE),
            javaSecurityConfigs,
            extraConfigs);
      } else {
        metaConfig = new HiveMetaConfig(
            config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_URL),
            config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_USERNAME),
            config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_PASSWORD),
            javaSecurityConfigs);
      }

      try {
        metaSource = ConnectorUtils.loadMetaSource(
            config.get(AbstractConfiguration.METADATA_SOURCE_CONNECTOR_PATH),
            metaConfig);
      } catch (Exception e) {
        throw new MmaException("Load Hive MetaSource FAIL", e);
      }
      return metaSource;
    }

    private void setUpType() {
      String impl = config.getOrDefault(AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL,
                                        AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL_DEFAULT_VALUE);
      if ("HMS".equalsIgnoreCase(impl)) {
        isHms = true;
      } else if ("JDBC".equalsIgnoreCase(impl)){
        isHms = false;
      } else {
        throw new IllegalArgumentException(
            "Unsupported value for " + AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL + ": " + impl);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      HiveMetaSourceInstance instance = (HiveMetaSourceInstance) o;

      if (isHms != instance.isHms) {
        return false;
      }

      List<String> checkList = new ArrayList<>();
      checkList.add(AbstractConfiguration.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
      checkList.add(AbstractConfiguration.JAVA_SECURITY_KRB5_CONF);
      checkList.add(AbstractConfiguration.JAVAX_SECURITY_AUTH_USESUBJECTCREDSONLY);
      if (isHms) {
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_META_STORE_EXTRA_CONFIGS);
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_URIS);
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_SASL_ENABLED);
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL);
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_KEYTAB_FILE);
      } else {
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_URL);
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_USERNAME);
        checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_PASSWORD);
      }

      for (String configName: checkList) {
        String s1 = config.get(configName);
        String s2 = instance.config.get(configName);
        if (s1 == null ^ s2 == null) {
          return false;
        } else if(s1 != null && !s1.equals(s2)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isHms);
    }
  }

  public static MetaSource getHiveMetaSource(AbstractConfiguration config) throws Exception {
    return HiveMetaSourceFactory.getHiveMetaSource(config);
  }

//  public static MetaSource getOdpsMetaSource(MmaConfig.OdpsConfig odpsConfig) {
//    return new OdpsMetaSource(
//        odpsConfig.getAccessId(),
//        odpsConfig.getAccessKey(),
//        odpsConfig.getEndpoint());
//  }
}
