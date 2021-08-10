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

//package com.aliyun.odps.datacarrier.taskscheduler.action;
//
//import com.aliyun.odps.datacarrier.taskscheduler.Constants;
//import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
//import java.util.HashMap;
//import java.util.Map;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//public class OdpsRestorePartitionAction extends OdpsSqlAction {
//  private static final Logger LOG = LogManager.getLogger(OdpsRestorePartitionAction.class);
//
//  private String backupName;
//  private String originProject;
//  private String originTableName;
//  private String destinationProject;
//  private String destinationTableName;
//  private Map<String, String> settings;
//
//  public OdpsRestorePartitionAction(
//      String id,
//      String backupName,
//      String originProject,
//      String originTableName,
//      String destinationProject,
//      String destinationTableName,
//      Map<String, String> settings) {
//    super(id);
//    this.backupName = backupName;
//    this.originProject = originProject;
//    this.originTableName = originTableName;
//    this.destinationProject = destinationProject;
//    this.destinationTableName = destinationTableName;
//    this.settings = settings;
//  }
//
//  @Override
//  String getSql() {
//    try {
//      String ossFileName = OssUtils.getOssPathToExportObject(
//          backupName,
//          Constants.EXPORT_TABLE_FOLDER,
//          originProject,
//          originTableName,
//          Constants.EXPORT_PARTITION_SPEC_FILE_NAME);
//
//      String content = OssUtils.readFile(actionExecutionContext.getOssConfig(), ossFileName);
//      LOG.info("Meta file {}, content {}", ossFileName, content);
//      StringBuilder builder = new StringBuilder();
//      builder.append("ALTER TABLE\n");
//      builder.append(destinationProject).append(".`").append(destinationTableName).append("`\n");
//      builder.append(content);
//      String sql = builder.toString();
//      LOG.info("Restore partitions from {}.{} to {}.{} as {}",
//               originProject, originTableName, destinationProject, destinationTableName, sql);
//
//      return sql;
//    } catch (Exception e) {
//      LOG.error("Restore partitions from {}.{} to {}.{} failed.",
//                originProject, originTableName, destinationProject, destinationTableName, e);
//      return "";
//    }
//  }
//
//  @Override
//  Map<String, String> getSettings() {
//    return (settings == null) ? new HashMap() : settings;
//  }
//
//  @Override
//  public String getName() {
//    return "Partition metadata restoration";
//  }
//}
