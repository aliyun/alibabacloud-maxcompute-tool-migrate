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

package com.aliyun.odps.mma.meta;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MetaSourceUtils {

  private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
  private static final String META_SOURCE_LOADER = "com.aliyun.odps.mma.meta.MetaSourceLoader";

  public static <T> T notNull(T object) {
    if (object == null) {
      throw new NullPointerException("The validated object is null");
    } else {
      return object;
    }
  }

  public static void loadHiveJdbc(String hiveConnectorJar)
      throws IOException, ClassNotFoundException, SQLException, InstantiationException,
             IllegalAccessException {
    ClassLoader targetClassLoader = MetaSourceClassLoader.getInstance(hiveConnectorJar);
    Driver d = (Driver) Class.forName(HIVE_DRIVER, true, targetClassLoader).newInstance();
    DriverManager.registerDriver(new MetaDriver(d));
  }

  public static MetaSource loadMetaSource(String hiveConnectorJar, MetaConfig config)
      throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
             MetaSourceLoadException {
    ClassLoader targetClassLoader = MetaSourceClassLoader.getInstance(hiveConnectorJar);
    Class<?> targetCls = Class.forName(META_SOURCE_LOADER, true, targetClassLoader);
    MetaLoader loader = (MetaLoader) targetCls.newInstance();
    return loader.load(config);
  }

}
