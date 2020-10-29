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

import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_COLUMN_TYPE_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_FOLDER;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_VIEW_FOLDER;

public class OdpsResetTableMetaModelAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsResetTableMetaModelAction.class);

  private String sourceDatabase;
  private String sourceTable;
  private MmaConfig.ObjectRestoreConfig restoreConfig;

  public OdpsResetTableMetaModelAction(String id,
                                       String sourceDatabase,
                                       String sourceTable,
                                       MmaConfig.ObjectRestoreConfig restoreConfig) {
    super(id);
    this.sourceDatabase = sourceDatabase;
    this.sourceTable = sourceTable;
    this.restoreConfig = restoreConfig;
  }

  @Override
  public void doAction() throws MmaException {
    try {
      MetaSource metaSource = MetaSourceFactory.getMetaSource();
      MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(sourceDatabase, sourceTable);
      LOG.info("GetTableMeta {}.{}, partitions {}", sourceDatabase, sourceTable, tableMetaModel.partitions.size());
      MmaConfig.TableMigrationConfig config = new MmaConfig.TableMigrationConfig(
          sourceDatabase,
          sourceTable,
          restoreConfig.getDestinationDatabaseName(),
          restoreConfig.getObjectName(),
          null);
      config.apply(tableMetaModel);
      String folder = MmaConfig.ObjectType.TABLE.equals(restoreConfig.getObjectType()) ? EXPORT_TABLE_FOLDER : EXPORT_VIEW_FOLDER;
      String ossFileName = OssUtils.getOssPathToExportObject(restoreConfig.getTaskName(),
          folder,
          restoreConfig.getOriginDatabaseName(),
          restoreConfig.getObjectName(),
          EXPORT_COLUMN_TYPE_FILE_NAME);
      String content = OssUtils.readFile(ossFileName);
      LOG.info("Meta file {}, content {}", ossFileName, content);
      Map<String, String> columnTypes = GsonUtils.getFullConfigGson().fromJson(content, GsonUtils.STRING_TO_STRING_MAP_TYPE);
      for(MetaSource.ColumnMetaModel columnMetaModel : tableMetaModel.columns) {
        if (columnTypes.containsKey(columnMetaModel.odpsColumnName)) {
          columnMetaModel.odpsType = columnTypes.get(columnMetaModel.odpsColumnName);
        } else {
          throw new MmaException("RestoreConfig: " + MmaConfig.ObjectRestoreConfig.toJson(restoreConfig)
              + ", Column: " + columnMetaModel.odpsColumnName + " not found in exported column types");
        }
      }
      for(MetaSource.ColumnMetaModel columnMetaModel : tableMetaModel.partitionColumns) {
        if (columnTypes.containsKey(columnMetaModel.odpsColumnName)) {
          columnMetaModel.odpsType = columnTypes.get(columnMetaModel.odpsColumnName);
        } else {
          throw new MmaException("RestoreConfig: " + MmaConfig.ObjectRestoreConfig.toJson(restoreConfig)
              + ", PartitionColumn: " + columnMetaModel.odpsColumnName + " not found in exported column types");
        }
      }
      actionExecutionContext.setTableMetaModel(tableMetaModel);
      LOG.info("Reset table meta model for {}, source {}.{}, destination {}.{}, partition size {}",
          id, tableMetaModel.databaseName, tableMetaModel.tableName,
          tableMetaModel.odpsProjectName, tableMetaModel.odpsTableName,
          tableMetaModel.partitions.size());
    } catch (Exception e) {
      LOG.error("Exception when reset table meta for task {}, table {}.{}, destination {}.{}",
          id, sourceDatabase, sourceTable,
          restoreConfig.getOriginDatabaseName(),
          restoreConfig.getObjectName(), e);
      throw new MmaException("Reset table meta fail for " + id, e);
    }
  }

  @Override
  public String getName() {
    return "Table metadata restoration";
  }
}
