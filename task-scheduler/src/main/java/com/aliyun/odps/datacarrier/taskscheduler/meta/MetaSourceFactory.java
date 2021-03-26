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

package com.aliyun.odps.datacarrier.taskscheduler.meta;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;

public class MetaSourceFactory {
  public static MetaSource getHiveMetaSource(
      MmaConfig.HiveConfig hiveConfig,
      Map<String, String> hdfsConfig) throws MetaException {

    return new HiveMetaSource(
        hiveConfig.getHmsThriftAddr(),
        (hdfsConfig != null) ? hdfsConfig: Collections.emptyMap(),
        hiveConfig.getKrbPrincipal(),
        hiveConfig.getKeyTab(),
        hiveConfig.getKrbSystemProperties());
  }

  public static MetaSource getOdpsMetaSource(MmaConfig.OdpsConfig odpsConfig) {
    return new OdpsMetaSource(
        odpsConfig.getAccessId(),
        odpsConfig.getAccessKey(),
        odpsConfig.getEndpoint(),
        odpsConfig.getProjectName());
  }
}
