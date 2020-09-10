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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.OssConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssExternalTableConfig;

public class OdpsCreateOssExternalTableAction extends OdpsSqlAction {

  private static final Logger LOG = LogManager.getLogger(OdpsCreateOssExternalTableAction.class);

  String ossFolder; // relative path from bucket, such as a/b/

  public OdpsCreateOssExternalTableAction(String id, String ossFolder) {
    super(id);
    this.ossFolder =  ossFolder;
  }

  @Override
  String getSql() {
    OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OssExternalTableConfig ossExternalTableConfig = new OssExternalTableConfig(
        ossConfig.getOssEndpoint(),
        ossConfig.getOssBucket(),
        ossConfig.getOssRoleArn(),
        ossFolder);

    LOG.info("OSS external table config: {}", GsonUtils.getFullConfigGson().toJson(ossExternalTableConfig));

    return OdpsSqlUtils.getCreateTableStatement(
        actionExecutionContext.getTableMetaModel(),
        ossExternalTableConfig);
  }

  @Override
  Map<String, String> getSettings() {
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getDDLSettings();
  }

}
