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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class MetaSourceFactory {

  // TODO: HACK avoid creating too many hive meta sources
  private MetaSource hiveMetaSource;

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
        if (hiveMetaSource == null) {
          hiveMetaSource = newHiveMetaSource(config);
        }
        return hiveMetaSource;
      }
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata source type: " + metadataSourceType);
    }
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
