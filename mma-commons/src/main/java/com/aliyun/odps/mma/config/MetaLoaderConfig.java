/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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
 *
 */

package com.aliyun.odps.mma.config;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.mma.meta.MetaDriver;
import com.aliyun.odps.mma.meta.MetaLoader;
import com.aliyun.odps.mma.meta.MetaSourceClassLoader;

public class MetaLoaderConfig {

  private static final Map<MetaSourceType, MetaLoader> META_LOADER_MAP = new HashMap<>();
  private static final Map<MetaSourceType, ClassLoader> CLASS_LOADER_MAP = new HashMap<>();
  private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  private static final String META_SOURCE_LOADER = "com.aliyun.odps.mma.meta.MetaSourceLoader";

  public static ClassLoader getClassLoader(MetaSourceType sourceType) {
    return CLASS_LOADER_MAP.get(sourceType);
  }

  public static MetaLoader getMetaLoader(MetaSourceType sourceType) {
    if (MetaSourceType.Hive.equals(sourceType)) {
      Thread.currentThread().setContextClassLoader(CLASS_LOADER_MAP.get(sourceType));
    }
    return META_LOADER_MAP.get(sourceType);
  }

  public static void setGlobalMetaLoader(AbstractConfiguration config)
      throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
             SQLException {
    String pathConfig = config.get(AbstractConfiguration.METADATA_SOURCE_CONNECTOR_PATH);
    for (String kvStr : pathConfig.split(";")) {
      String[] kv = kvStr.split(":");
      MetaSourceType metaSourceType = MetaSourceType.valueOf(kv[0]);
      String jar = kv[1];

      ClassLoader targetClassLoader = MetaSourceClassLoader.getInstance(jar);
      Class<?> targetCls = Class.forName(META_SOURCE_LOADER, true, targetClassLoader);
      MetaLoader loader = (MetaLoader) targetCls.newInstance();

      CLASS_LOADER_MAP.put(metaSourceType, targetClassLoader);
      META_LOADER_MAP.put(metaSourceType, loader);
    }

    ClassLoader targetClassLoader = CLASS_LOADER_MAP.get(MetaSourceType.Hive);
    Driver d = (Driver) Class.forName(HIVE_DRIVER, true, targetClassLoader).newInstance();
    DriverManager.registerDriver(new MetaDriver(d));
  }

}
