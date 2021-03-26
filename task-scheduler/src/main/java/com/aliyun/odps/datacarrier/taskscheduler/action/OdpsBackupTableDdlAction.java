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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssExternalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;

public class OdpsBackupTableDdlAction extends OdpsSqlAction {

  private static final Logger LOG = LogManager.getLogger(OdpsBackupTableDdlAction.class);

  private String backupName;
  private AtomicInteger lineageTasksCounter;
  private MetaSource.TableMetaModel tableMetaModel;

  public OdpsBackupTableDdlAction(
      String id,
      String backupName,
      MetaSource.TableMetaModel tableMetaModel,
      AtomicInteger lineageTasksCounter) {
    super(id);
    this.backupName = backupName;
    this.tableMetaModel = tableMetaModel;
    this.lineageTasksCounter = lineageTasksCounter;
    this.lineageTasksCounter.incrementAndGet();
  }

  public void backupDdlStatement() {
    String location = OssUtils.getOssPathToExportObject(
        backupName,
        "tables/",
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        "data/");

    MmaConfig.OssConfig ossConfig = actionExecutionContext.getOssConfig();

    OssExternalTableConfig ossExternalTableConfig = new OssExternalTableConfig(
        ossConfig.getOssEndpoint(),
        ossConfig.getOssBucket(),
        ossConfig.getOssRoleArn(),
        OdpsSqlUtils.getOssTablePath(ossConfig, location));
    String statement = OdpsSqlUtils.getCreateTableStatementWithoutDatabaseName(tableMetaModel, ossExternalTableConfig);

    LOG.debug("Action {}, Task {}, export table {}.{}, statement {}",
              id, backupName, tableMetaModel.databaseName, tableMetaModel.tableName, statement);

    String ossFileName = OssUtils.getOssPathToExportObject(
        backupName,
        "tables/",
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        "meta");

    OssUtils.createFile(ossConfig, ossFileName, statement);

    Map<String, String> columnTypes = new HashMap<String, String>();
    for (int i = 0; i < tableMetaModel.columns.size(); i++) {
      MetaSource.ColumnMetaModel columnMetaModel = tableMetaModel.columns.get(i);
      columnTypes.put(columnMetaModel.odpsColumnName, columnMetaModel.type);
    }
    for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
      MetaSource.ColumnMetaModel columnMetaModel = tableMetaModel.partitionColumns.get(i);
      columnTypes.put(columnMetaModel.odpsColumnName, columnMetaModel.type);
    }
    String columnTypesContent = GsonUtils.toJson(columnTypes);
    LOG.info("Origin column types {}", columnTypesContent);
    ossFileName = OssUtils.getOssPathToExportObject(
        backupName,
        "tables/",
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        "column_type");

    OssUtils.createFile(ossConfig, ossFileName, columnTypesContent);

    if (!tableMetaModel.partitionColumns.isEmpty() && !tableMetaModel.partitions.isEmpty()) {
      String addPartitionStatement =
          OdpsSqlUtils.getAddPartitionStatementWithoutDatabaseName(tableMetaModel);
      ossFileName = OssUtils.getOssPathToExportObject(
          backupName,
          "tables/",
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          "partition_spec");

      OssUtils.createFile(ossConfig, ossFileName, addPartitionStatement);
      LOG.debug("Action {}, Task {}, export partition spec {}.{}, statement {}",
                id, backupName, tableMetaModel.databaseName, tableMetaModel.tableName, addPartitionStatement);
    }
  }

  @Override
  String getSql() {
    int remainTasks = lineageTasksCounter.decrementAndGet();
    LOG.info("Action {}, remain tasks {}", id, remainTasks);
    if (remainTasks == 0) {
      backupDdlStatement();
      return OdpsSqlUtils.getDropTableStatement(
          tableMetaModel.odpsProjectName,
          tableMetaModel.odpsTableName);
    }

    return "";
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
    return "Table metadata backup";
  }
}
