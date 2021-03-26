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

import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;

public class OdpsAddPartitionAction extends OdpsSqlAction {

  public OdpsAddPartitionAction(String id) {
    super(id);
    resourceMap.put(Resource.MC_METADATA_OPERATION_RESOURCE, 1L);
  }

  @Override
  String getSql() {
    return OdpsSqlUtils.getAddPartitionStatement(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    return actionExecutionContext
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getDDLSettings();
  }

  @Override
  public String getName() {
    return "Partition creation";
  }
}
