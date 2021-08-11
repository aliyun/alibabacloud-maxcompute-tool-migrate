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

package com.aliyun.odps.mma.meta;

import java.util.*;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class MetaSourceFactory {

  private static final int MAX_HIVE_META_SOURCE_NUM = 5;
  private List<AbstractConfiguration> hiveConfigQueue = new LinkedList<>();
  private Map<AbstractConfiguration, MetaSource> hiveConfig2MetaSource = new HashMap<>();

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
        return new OssMetaSource(
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_ID),
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_SECRET),
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_BUCKET),
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_PATH),
            config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT));
      }
      case "Hive": {
        for (AbstractConfiguration existConfig : hiveConfigQueue) {
          if (hiveConfigEqual(existConfig, config)) {
            hiveConfigQueue.remove(existConfig);
            hiveConfigQueue.add(existConfig);
            return hiveConfig2MetaSource.get(existConfig);
          }
        }

        if (hiveConfigQueue.size() >= MAX_HIVE_META_SOURCE_NUM) {
          hiveConfig2MetaSource.remove(hiveConfigQueue.get(0));
          hiveConfigQueue.remove(0);
        }

        hiveConfigQueue.add(config);
        MetaSource hiveMetaSource = newHiveMetaSource(config);
        hiveConfig2MetaSource.put(config, hiveMetaSource);
        return hiveMetaSource;
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata source type: " + metadataSourceType);
    }
  }

  private static boolean hiveConfigEqual(AbstractConfiguration conf1, AbstractConfiguration conf2) {
    String impl1 = conf1.getOrDefault(AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL,
        AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL_DEFAULT_VALUE);
    String impl2 = conf2.getOrDefault(AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL,
        AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL_DEFAULT_VALUE);
    if (!impl1.equals(impl2)) return false;

    List<String> checkList = new ArrayList<>();
    checkList.add(AbstractConfiguration.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
    checkList.add(AbstractConfiguration.JAVA_SECURITY_KRB5_CONF);
    checkList.add(AbstractConfiguration.JAVAX_SECURITY_AUTH_USESUBJECTCREDSONLY);
    if (impl1.equalsIgnoreCase("HMS")) {
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_META_STORE_EXTRA_CONFIGS);
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_URIS);
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_SASL_ENABLED);
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL);
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_KEYTAB_FILE);
    } else if (impl1.equalsIgnoreCase("JDBC")) {
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_URL);
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_USERNAME);
      checkList.add(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_PASSWORD);
    } else {
      throw new IllegalArgumentException(
          "Unsupported value for " + AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL + ": " + impl1);
    }

    for(String config: checkList) {
      if(!conf1.get(config).equals(conf2.get(config))) return false;
    }
    return true;
  }

  private static MetaSource newHiveMetaSource(
      AbstractConfiguration config) throws Exception {
    String impl = config.getOrDefault(
        AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL,
        AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL_DEFAULT_VALUE);

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

    if ("HMS".equalsIgnoreCase(impl)) {
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
      return new HiveMetaSourceHmsImpl(
          config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_URIS),
          Boolean.valueOf(config.get(
              AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_SASL_ENABLED)),
          config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL),
          config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_KERBEROS_KEYTAB_FILE),
          javaSecurityConfigs,
          extraConfigs);
    } else if ("JDBC".equalsIgnoreCase(impl)) {
      return new HiveMetaSourceJdbcImpl(
          config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_URL),
          config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_USERNAME),
          config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_JDBC_PASSWORD),
          javaSecurityConfigs);
    } else {
      throw new IllegalArgumentException(
          "Unsupported value for " + AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL + ": " + impl);
    }
  }

  public static MetaSource getHiveMetaSource(AbstractConfiguration config) throws Exception {
    return newHiveMetaSource(config);
  }

//  public static MetaSource getOdpsMetaSource(MmaConfig.OdpsConfig odpsConfig) {
//    return new OdpsMetaSource(
//        odpsConfig.getAccessId(),
//        odpsConfig.getAccessKey(),
//        odpsConfig.getEndpoint());
//  }
}
