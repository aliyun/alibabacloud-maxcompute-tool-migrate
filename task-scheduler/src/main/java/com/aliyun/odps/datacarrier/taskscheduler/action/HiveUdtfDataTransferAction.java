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
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.HiveSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;
import com.aliyun.odps.datacarrier.taskscheduler.resource.ResourceAllocator;

public class HiveUdtfDataTransferAction extends HiveSqlAction {

  private static final Logger LOG = LogManager.getLogger(HiveUdtfDataTransferAction.class);

  public HiveUdtfDataTransferAction(String id) {
    super(id);
    // Init default resourceMap
    resourceMap.put(Resource.HIVE_DATA_TRANSFER_JOB_RESOURCE, 1L);
    resourceMap.put(Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE, 5L);
  }

  @Override
  String getSql() {
    return HiveSqlUtils.getUdtfSql(actionExecutionContext.getTableMetaModel());
  }

  @Override
  public boolean tryAllocateResource() {
    TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
    boolean isPartitioned = !tableMetaModel.partitionColumns.isEmpty();

    // Get total data size
    Long totalDataSize = null;
    if (isPartitioned) {
      Optional<Long> optionalTotalDataSize = tableMetaModel.partitions
          .stream()
          .map(p -> p.size).reduce((s1, s2) -> s1 + s2);
      if (optionalTotalDataSize.isPresent()) {
        totalDataSize = optionalTotalDataSize.get();
      }
    } else {
      if (tableMetaModel.size != null) {
        totalDataSize = tableMetaModel.size;
      }
    }

    // Update resource map based on total data size
    if (totalDataSize != null) {
      long numHiveDataTransferWorkerResource =
          Math.max(1L, totalDataSize / Constants.DEFAULT_MAPREDUCE_SPLIT_SIZE_IN_BYTE);
      resourceMap.put(
          Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE, numHiveDataTransferWorkerResource);
      LOG.info("ActionId: {}, data size: {}, updated resource map: {}",
               id,
               totalDataSize,
               resourceMap.toString());
    } else {
      LOG.warn("ActionId: {}, failed to get accurate data size, resource map: {}",
               id,
               resourceMap.toString());
    }

    Map<Resource, Long> finalResourceMap =
        ResourceAllocator.getInstance().allocate(id, resourceMap);
    if (finalResourceMap != null) {
      resourceMap = finalResourceMap;
      return true;
    }

    return false;
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

  @Override
  public String getName() {
    return "Data transmission";
  }
}
