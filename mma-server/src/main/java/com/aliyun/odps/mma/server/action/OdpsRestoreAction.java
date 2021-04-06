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

package com.aliyun.odps.mma.server.action;

//import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
//import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
//import org.apache.commons.lang.exception.ExceptionUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//public abstract class OdpsRestoreAction extends DefaultAction {
//  private static final Logger LOG = LogManager.getLogger(OdpsRestoreAction.class);
//
//  MmaConfig.ObjectRestoreConfig restoreConfig;
//
//  public OdpsRestoreAction(String id, MmaConfig.ObjectRestoreConfig restoreConfig) {
//    super(id);
//    this.restoreConfig = restoreConfig;
//  }
//
//  public String getBackupName() {
//    return restoreConfig.getBackupName();
//  }
//
//  public String getSourceProject() {
//    return restoreConfig.getSourceDatabaseName();
//  }
//
//  public String getDestinationProject() {
//    return restoreConfig.getDestinationDatabaseName();
//  }
//
//  public MmaConfig.ObjectType getObjectType() {
//    return restoreConfig.getObjectType();
//  }
//
//  public String getObjectName() {
//    return restoreConfig.getObjectName();
//  }
//
//  public boolean isUpdate() {
//    return restoreConfig.isUpdate();
//  }
//
//  @Override
//  public Object call() throws MmaException {
//    try {
//      restore();
//    } catch (Exception e) {
//      LOG.error("Action {} failed, object type: {} object: {}, stack trace: {}", id, restoreConfig.getObjectType(), restoreConfig.getObjectName(), ExceptionUtils.getFullStackTrace(e));
//      setProgress(ActionProgress.FAILED);
//    }
//
//    return null;
//  }
//
//  abstract void restore() throws Exception;
//
//  public String getRestoredFilePath(String folderName, String fileName) throws Exception {
//    String ossFileName = OssUtils.getOssPathToExportObject(
//        getBackupName(),
//        folderName,
//        getSourceProject(),
//        getObjectName(),
//        fileName);
//
//    if (!OssUtils.exists(restoreConfig.getOssConfig(), ossFileName)) {
//      String errorMsg = String.format("ActionId: %s, OSS file %s not found", id, ossFileName);
//      throw new MmaException(errorMsg);
//    }
//    return ossFileName;
//  }
//}
