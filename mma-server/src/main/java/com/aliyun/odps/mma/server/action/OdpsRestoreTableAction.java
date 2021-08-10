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
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import com.aliyun.odps.datacarrier.taskscheduler.Constants;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
//import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
//import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
//
//
//public class OdpsRestoreTableAction extends OdpsSqlAction {
//  private static final Logger LOG = LogManager.getLogger(OdpsRestoreTableAction.class);
//
//  private String backupName;
//  private String originProject;
//  private String tableName;
//  private String destinationProject;
//  private String destinationTable;
//  private MmaConfig.ObjectType type;
//  private Map<String, String> settings;
//
//  public OdpsRestoreTableAction(
//      String id,
//      String backupName,
//      String originProject,
//      String originTable,
//      String destinationProject,
//      String destinationTable,
//      MmaConfig.ObjectType type,
//      Map<String, String> settings) {
//    super(id);
//    this.backupName = backupName;
//    this.originProject = originProject;
//    this.destinationProject = destinationProject;
//    this.type = type;
//    this.tableName = originTable;
//    this.destinationTable = destinationTable;
//    this.settings = settings;
//  }
//
//  @Override
//  String getSql() {
//    try {
//      String folder = MmaConfig.ObjectType.TABLE.equals(type) ?
//          Constants.EXPORT_TABLE_FOLDER : Constants.EXPORT_VIEW_FOLDER;
//      String ossFileName = OssUtils.getOssPathToExportObject(
//          backupName, folder, originProject, tableName, Constants.EXPORT_META_FILE_NAME);
//
//      String content = OssUtils.readFile(actionExecutionContext.getOssConfig(), ossFileName);
//      LOG.debug("Meta file {}, content {}", ossFileName, content);
//      StringBuilder builder = new StringBuilder();
//      if (MmaConfig.ObjectType.VIEW.equals(type)) {
//        builder.append(OdpsSqlUtils.getCreateViewStatement(destinationProject, tableName, content));
//      } else {
//        builder.append("CREATE EXTERNAL TABLE IF NOT EXISTS ")
//               .append(destinationProject).append(".")
//               .append("`").append(destinationTable).append("`")
//               .append(content);
//      }
//      String sql = builder.toString();
//      LOG.debug("Restore {} from {}.{} to {}.{} as {}", type.name(), originProject, tableName, destinationProject, MmaConfig.ObjectType.VIEW.equals(type) ? tableName : destinationTable, sql);
//
//      return sql;
//    } catch (Exception e) {
//      LOG.error("Restore {} {} from {}.{} to {}.{} failed.", type.name(), originProject, tableName, destinationProject, MmaConfig.ObjectType.VIEW.equals(type) ? tableName : destinationTable, e);
//
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
//    return "Table metadata restoration";
//  }
//}