package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssExternalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_COLUMN_TYPE_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_PARTITION_SPEC_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_FOLDER;

public class OdpsExportTableDDLAction extends OdpsSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportTableDDLAction.class);

  private String taskName;

  /**
   * for partition table, job will be split to multi-tasks, DDL statement should be exported when
   * all tasks is finished
   */
  private AtomicInteger lineageTasksCounter;
  private MetaSource.TableMetaModel tableMetaModel;

  public OdpsExportTableDDLAction(String id,
                                  String taskName,
                                  MetaSource.TableMetaModel tableMetaModel,
                                  AtomicInteger lineageTasksCounter) {
    super(id);
    this.taskName = taskName;
    this.tableMetaModel = tableMetaModel;
    this.lineageTasksCounter = lineageTasksCounter;
    this.lineageTasksCounter.incrementAndGet();
  }

  public void exportDDLStatement() {
    String location = OssUtils.getOssPathToExportObject(
            taskName,
            Constants.EXPORT_TABLE_FOLDER,
            tableMetaModel.databaseName,
            tableMetaModel.tableName,
            Constants.EXPORT_TABLE_DATA_FOLDER);
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OssExternalTableConfig ossExternalTableConfig = new OssExternalTableConfig(
        ossConfig.getOssEndpoint(),
        ossConfig.getOssBucket(),
        ossConfig.getOssRoleArn(),
        OdpsSqlUtils.getOssTablePath(ossConfig, location));
    String statement = OdpsSqlUtils.getCreateTableStatementWithoutDatabaseName(
        tableMetaModel, ossExternalTableConfig);
    LOG.info("Action {}, Task {}, export table {}.{}, statement {}",
        id, taskName, tableMetaModel.databaseName, tableMetaModel.tableName, statement);
    String ossFileName = OssUtils.getOssPathToExportObject(taskName,
        EXPORT_TABLE_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        EXPORT_META_FILE_NAME);
    OssUtils.createFile(ossFileName, statement);

    // we must transfer column type to TIMESTAME if corresponding internal table column type is DATETIME
    // so origin column types should be recorded for restore task
    Map<String, String> columnTypes = new HashMap<>();
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
    ossFileName = OssUtils.getOssPathToExportObject(taskName,
        EXPORT_TABLE_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        EXPORT_COLUMN_TYPE_FILE_NAME);
    OssUtils.createFile(ossFileName, columnTypesContent);

    if (!tableMetaModel.partitionColumns.isEmpty() && !tableMetaModel.partitions.isEmpty()) {
      String addPartitionStatement = OdpsSqlUtils.getAddPartitionStatementWithoutDatabaseName(tableMetaModel);
      ossFileName = OssUtils.getOssPathToExportObject(taskName,
          EXPORT_TABLE_FOLDER,
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          EXPORT_PARTITION_SPEC_FILE_NAME);
      OssUtils.createFile(ossFileName, addPartitionStatement);
      LOG.info("Action {}, Task {}, export partition spec {}.{}, statement {}",
        id, taskName, tableMetaModel.databaseName, tableMetaModel.tableName, addPartitionStatement);
    }
  }

  @Override
  String getSql() {
    int remainTasks = lineageTasksCounter.decrementAndGet();
    LOG.info("Action {}, remain tasks {}", id, remainTasks);
    if (remainTasks == 0) {
      exportDDLStatement();
      return OdpsSqlUtils.getDropTableStatement(tableMetaModel.odpsProjectName, tableMetaModel.odpsTableName);
    }
    return "";
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
