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

package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.HiveSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;

public class HiveUdtfDataTransferAction extends HiveSqlAction {

  private static final Logger LOG = LogManager.getLogger(HiveUdtfDataTransferAction.class);

  public HiveUdtfDataTransferAction(String id) {
    super(id);
    // Init default resourceMap
    resourceMap.put(Resource.HIVE_DATA_TRANSFER_JOB_RESOURCE, 1);
    resourceMap.put(Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE, 5);
  }

  @Override
  String getSql() {
    return HiveSqlUtils.getUdtfSql(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    Map<String, String> settings = new HashMap<>(
        MmaServerConfig
            .getInstance()
            .getHiveConfig()
            .getSourceTableSettings()
            .getMigrationSettings());

    if (resourceMap.containsKey(Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE)) {
      settings.put("mapreduce.job.running.map.limit",
                   resourceMap.get(Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE).toString());
    }
    return settings;
  }
}
