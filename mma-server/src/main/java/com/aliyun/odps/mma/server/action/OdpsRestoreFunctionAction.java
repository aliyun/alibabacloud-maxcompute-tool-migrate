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

//package com.aliyun.odps.mma.server.action;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import com.aliyun.odps.datacarrier.taskscheduler.Constants;
//import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
//import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
//import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;

//public class OdpsRestoreFunctionAction extends OdpsRestoreAction {
//  private static final Logger LOG = LogManager.getLogger(OdpsRestoreFunctionAction.class);
//
//  public OdpsRestoreFunctionAction(
//      String id,
//      MmaConfig.ObjectRestoreConfig restoreConfig) {
//    super(id, restoreConfig);
//  }
//
//  @Override
//  public void restore() throws Exception {
//    String ossFileName = getRestoredFilePath(
//        Constants.EXPORT_FUNCTION_FOLDER,
//        Constants.EXPORT_META_FILE_NAME);
//    String content = OssUtils.readFile(restoreConfig.getOssConfig(), ossFileName);
//
//    OdpsFunctionInfo functionInfo = GsonUtils.getFullConfigGson().fromJson(
//        content,
//        OdpsFunctionInfo.class);
//
//    OdpsUtils.createFunction(
//        restoreConfig.getOdpsConfig(),
//        getDestinationProject(),
//        functionInfo,
//        isUpdate());
//    LOG.info("Restore function {} from {} to {}",
//             functionInfo.getFunctionName(),
//             getSourceProject(),
//             getDestinationProject());
//  }
//
//
//  @Override
//  public String getName() { return "Function restoration"; }
//}
