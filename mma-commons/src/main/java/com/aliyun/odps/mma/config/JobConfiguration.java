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

package com.aliyun.odps.mma.config;

import java.util.Map;

import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class JobConfiguration extends AbstractConfiguration {
  /**
   * Object type. Could be TABLE, VIRTUAL_VIEW, PARTITION, RESOURCE, FUNCTION.
   */
  public static final String OBJECT_TYPE = "mma.object.type";

  /**
   * Source object position.
   */
  public static final String SOURCE_CATALOG_NAME = "mma.object.source.catalog.name";
  public static final String SOURCE_OBJECT_TYPES = "mma.object.source.types";
//  public static final String SOURCE_SCHEMA_NAME = "mma.object.source.schema.name";
  public static final String SOURCE_OBJECT_NAME = "mma.object.source.name";

  /**
   * Source Object attributes.
   */
  // Last modified time of the source object when the job is created or reset. Can be used to
  // determine whether the source object has changed. The job configuration would not contain this
  // key when the last modified time is not valid.
  public static final String SOURCE_OBJECT_LAST_MODIFIED_TIME = "mma.object.mtime";

  /**
   * Dest object position.
   */
  public static final String DEST_CATALOG_NAME = "mma.object.dest.catalog.name";
//  public static final String DEST_SCHEMA_NAME = "mma.object.dest.schema.name";
  public static final String DEST_OBJECT_NAME = "mma.object.dest.name";

  /**
   * Job attributes.
   */
  public static final String JOB_ID = "mma.job.id";

  public JobConfiguration(Map<String, String> configuration) {
    super(configuration);
  }

  public static JobConfiguration fromJson(String json) {
    Map<String, String> map =
        GsonUtils.GSON.fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
    return new JobConfiguration(map);
  }

  @Override
  public void validate() throws MmaException {
    // Supported source and destination combinations:
    // 1. Hive(metadata), Hive(data) -> MC(metadata), MC(data)
    // 2. MC(metadata), MC(metadata) -> OSS(metadata), OSS(data)
    // 3. OSS(metadata), OSS(data) -> MC(metadata), MC(data)
    MetaSourceType metaSourceType = MetaSourceType.valueOf(configuration.get(METADATA_SOURCE_TYPE));
    DataSourceType dataSourceType = DataSourceType.valueOf(configuration.get(DATA_SOURCE_TYPE));
    MetaDestType metaDestType = MetaDestType.valueOf(configuration.get(METADATA_DEST_TYPE));
    DataDestType dataDestType = DataDestType.valueOf(configuration.get(DATA_DEST_TYPE));
    if (metaSourceType.equals(MetaSourceType.MaxCompute)
        && dataSourceType.equals(DataSourceType.MaxCompute)
        && metaDestType.equals(MetaDestType.OSS)
        && dataDestType.equals(DataDestType.OSS)) {
      validateMcToOssCredentials();
    } else if (metaSourceType.equals(MetaSourceType.OSS)
        && dataSourceType.equals(DataSourceType.OSS)
        && metaDestType.equals(MetaDestType.MaxCompute)
        && dataDestType.equals(DataDestType.MaxCompute)) {
      validateOssToMcCredentials();
    } else if (metaSourceType.equals(MetaSourceType.Hive)
        && dataSourceType.equals(DataSourceType.Hive)
        && metaDestType.equals(MetaDestType.MaxCompute)
        && dataDestType.equals(DataDestType.MaxCompute)) {
      validateHiveToMcCredentials();
    } else {
      throw new IllegalArgumentException("Unsupported source and dest combination.");
    }
  }

  private void validateMcToOssCredentials() {
    // TODO:
  }

  private void validateOssToMcCredentials() {
    // TODO:
  }

  private void validateHiveToMcCredentials() {
    // TODO:
  }
}
