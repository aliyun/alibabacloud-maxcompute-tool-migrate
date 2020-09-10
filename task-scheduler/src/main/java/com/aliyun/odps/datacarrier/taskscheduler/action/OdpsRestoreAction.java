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

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class OdpsRestoreAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsRestoreAction.class);

  private MmaConfig.ObjectRestoreConfig restoreConfig;

  public OdpsRestoreAction(String id, MmaConfig.ObjectRestoreConfig restoreConfig) {
    super(id);
    this.restoreConfig = restoreConfig;
  }

  public String getTaskName() {
    return restoreConfig.getTaskName();
  }

  public String getOriginProject() {
    return restoreConfig.getOriginDatabaseName();
  }

  public String getDestinationProject() {
    return restoreConfig.getDestinationDatabaseName();
  }

  public MmaConfig.ObjectType getObjectType() {
    return restoreConfig.getObjectType();
  }

  public String getObjectName() {
    return restoreConfig.getObjectName();
  }

  public boolean isUpdate() {
    return restoreConfig.isUpdate();
  }

  @Override
  public void doAction() throws MmaException {
    try {
      restore();
    } catch (Exception e) {
      LOG.error("Action {} failed, restore type: {} object: {} from {} to {}, update {}, stack trace: {}",
          id, restoreConfig.getObjectType(), restoreConfig.getObjectName(), restoreConfig.getOriginDatabaseName(),
          restoreConfig.getDestinationDatabaseName(), restoreConfig.isUpdate(), ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  abstract void restore() throws Exception;

  public String getRestoredFilePath(String folderName, String fileName) throws Exception {
    String ossFileName = OssUtils.getOssPathToExportObject(getTaskName(),
        folderName,
        getOriginProject(),
        getObjectName(),
        fileName);
    if (!OssUtils.exists(ossFileName)) {
      LOG.error("Oss file {} not found", ossFileName);
      throw new MmaException("Oss file " + ossFileName + " not found when restore " + getObjectType().name()
          + " " + getObjectName() + " from " + getOriginProject() + " to " + getDestinationProject());
    }
    return ossFileName;
  }
}
