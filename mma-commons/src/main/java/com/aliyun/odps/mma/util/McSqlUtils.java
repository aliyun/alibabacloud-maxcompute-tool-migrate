/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.util;

import java.nio.file.Paths;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.Constants;
import com.aliyun.odps.mma.config.ExternalTableStorage;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.meta.MetaSource.ColumnMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel.TableMetaModelBuilder;
import com.aliyun.odps.utils.StringUtils;

public class McSqlUtils {

  static final Logger LOG = LogManager.getLogger(McSqlUtils.class);

  public static String getDropTableStatement(TableMetaModel tableMetaModel) {
    return "DROP TABLE IF EXISTS " + getCoordinate(tableMetaModel) + ";\n";
  }

  public static String getCreateTableStatement(TableMetaModel tableMetaModel) {
    return getCreateTableStatement(tableMetaModel, null);
  }

  public static String getCreateTableStatement(
      TableMetaModel tableMetaModel, ExternalTableStorage storage) {
    StringBuilder sb = new StringBuilder();
    if (storage != null) {
      sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
    } else {
      sb.append("CREATE TABLE IF NOT EXISTS ");
    }
    sb.append(getCoordinate(tableMetaModel));
    sb.append(getCreateTableStatementWithoutDatabaseName(tableMetaModel, storage));
    return sb.toString();
  }

  private static String getCreateTableStatementWithoutDatabaseName(
      TableMetaModel tableMetaModel, ExternalTableStorage storage) {
    StringBuilder sb = new StringBuilder("(\n");
    for (int i = 0; i < tableMetaModel.getColumns().size(); i++) {
      ColumnMetaModel columnMetaModel = tableMetaModel.getColumns().get(i);
      sb.append("    `").append(columnMetaModel.getColumnName()).append("` ")
        .append(columnMetaModel.getType());

      if (columnMetaModel.getComment() != null) {
        sb.append(" COMMENT '").append(columnMetaModel.getComment()).append("'");
      }

      if (i + 1 < tableMetaModel.getColumns().size()) {
        sb.append(",\n");
      }
    }

    sb.append("\n)");

    if (tableMetaModel.getComment() != null) {
      sb.append("\nCOMMENT '").append(tableMetaModel.getComment()).append("'\n");
    }

    if (!tableMetaModel.getPartitionColumns().isEmpty()) {
      sb.append("\nPARTITIONED BY (\n");
      for (int i = 0; i < tableMetaModel.getPartitionColumns().size(); i++) {
        ColumnMetaModel partitionColumnMetaModel =
            tableMetaModel.getPartitionColumns().get(i);
        sb.append("    `").append(partitionColumnMetaModel.getColumnName()).append("` ")
          .append(partitionColumnMetaModel.getType());

        if (partitionColumnMetaModel.getComment() != null) {
          sb.append(" COMMENT '").append(partitionColumnMetaModel.getComment()).append("'");
        }

        if (i + 1 < tableMetaModel.getPartitionColumns().size()) {
          sb.append(",\n");
        }
      }
      sb.append("\n)");
    }

    if (storage != null) {
      switch (storage) {
        case OSS:
          sb.append(getCreateOssExternalTableCondition(tableMetaModel));
          break;
        default:
          throw new IllegalArgumentException("Unknown external table storage: " + storage.name());
      }
    }

    sb.append(";\n");

    return sb.toString();
  }

  private static String getCreateOssExternalTableCondition(TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();

    // TODO: support other formats & compression methods
    String fileType = "ORC";
    if (!StringUtils.isNullOrEmpty(tableMetaModel.getSerDe())) {
      fileType = tableMetaModel.getSerDe();
    }
    sb.append("\nSTORED AS " + fileType)
        .append("\nLOCATION '").append(tableMetaModel.getLocation()).append("'");
    return sb.toString();
  }

  /**
   * Get drop partition statement
   *
   * @param tableMetaModel {@link TableMetaModel}
   * @return Drop partition statement for multiple partitions
   * @throws IllegalArgumentException when input represents a non partitioned table
   */
  public static String getDropPartitionStatement(TableMetaModel tableMetaModel) {
    if (tableMetaModel.getPartitionColumns().isEmpty()) {
      throw new IllegalArgumentException("Not a partitioned table");
    }

    if (tableMetaModel.getPartitions().size() > Constants.MAX_PARTITION_GROUP_SIZE) {
      throw new IllegalArgumentException(
          "Partition group size exceeds upper bound: " + Constants.MAX_PARTITION_GROUP_SIZE);
    }

    StringBuilder sb = new StringBuilder();
    if (tableMetaModel.getPartitions().isEmpty()) {
      return sb.toString();
    }

    sb.append("ALTER TABLE\n");
    sb.append(getCoordinate(tableMetaModel));
    sb.append("\n");
    sb.append("DROP IF EXISTS");
    for (int i = 0; i < tableMetaModel.getPartitions().size(); i++) {
      PartitionMetaModel partitionMetaModel = tableMetaModel.getPartitions().get(i);
      String partitionSpec = getPartitionSpec(
          tableMetaModel.getPartitionColumns(), partitionMetaModel);
      sb.append("\nPARTITION (").append(partitionSpec).append(")");
      if (i != tableMetaModel.getPartitions().size() - 1) {
        sb.append(",");
      }
    }
    sb.append(";\n");

    return sb.toString();
  }

  /**
   * Get add partition statement
   *
   * @param tableMetaModel {@link TableMetaModel}
   * @return Add partition statement for multiple partitions
   * @throws IllegalArgumentException when input represents a non partitioned table
   */
  public static String getAddPartitionStatement(TableMetaModel tableMetaModel) {
    if (tableMetaModel.getPartitionColumns().isEmpty()) {
      throw new IllegalArgumentException("Not a partitioned table");
    }

    if (tableMetaModel.getPartitions().size() > Constants.MAX_PARTITION_GROUP_SIZE) {
      throw new IllegalArgumentException(
          "Partition group size exceeds upper bound: " + Constants.MAX_PARTITION_GROUP_SIZE);
    }

    StringBuilder sb = new StringBuilder();
    if (tableMetaModel.getPartitions().isEmpty()) {
      return sb.toString();
    }

    sb.append("ALTER TABLE\n");
    sb.append(getCoordinate(tableMetaModel));
    sb.append("\n");
    sb.append(getAddPartitionStatementWithoutDatabaseName(tableMetaModel));
    return sb.toString();
  }

  private static String getAddPartitionStatementWithoutDatabaseName(
      TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();
    sb.append("ADD IF NOT EXISTS");
    for (PartitionMetaModel partitionMetaModel : tableMetaModel.getPartitions()) {
      String partitionSpec =
          getPartitionSpec(tableMetaModel.getPartitionColumns(), partitionMetaModel);
      sb.append("\nPARTITION (").append(partitionSpec).append(")");
    }
    sb.append(";\n");

    return sb.toString();
  }

  public static String getInsertOverwriteTableStatement(
      TableMetaModel source,
      TableMetaModel dest) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT OVERWRITE TABLE ").append(getCoordinate(dest)).append("\n");
    if (!dest.getPartitionColumns().isEmpty()) {
      sb.append("PARTITION (");
      for (int i = 0; i < dest.getPartitionColumns().size(); i++) {
        ColumnMetaModel c = dest.getPartitionColumns().get(i);
        sb.append("`").append(c.getColumnName()).append("`");
        if (i != dest.getPartitionColumns().size() - 1) {
          sb.append(", ");
        }
      }
      sb.append(")\n");
    }
    sb.append("SELECT * FROM ").append(getCoordinate(source)).append("\n");
    sb.append(getWhereCondition(source));
    sb.append(";\n");
    return sb.toString();
  }

  public static String getVerifySql(TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");

    if (!tableMetaModel.getPartitionColumns().isEmpty()) {
      for (int i = 0; i < tableMetaModel.getPartitionColumns().size(); i++) {
        ColumnMetaModel columnMetaModel = tableMetaModel.getPartitionColumns().get(i);
        sb.append("`").append(columnMetaModel.getColumnName()).append("`");
        sb.append(", ");
      }
    }

    sb.append("COUNT(1) FROM\n");
    sb.append(getCoordinate(tableMetaModel));
    sb.append("\n");

    if (!tableMetaModel.getPartitionColumns().isEmpty()) {
      String whereCondition = getWhereCondition(tableMetaModel);
      sb.append(whereCondition);

      sb.append("\nGROUP BY ");
      for (int i = 0; i < tableMetaModel.getPartitionColumns().size(); i++) {
        ColumnMetaModel c = tableMetaModel.getPartitionColumns().get(i);
        sb.append("`").append(c.getColumnName()).append("`");
        if (i != tableMetaModel.getPartitionColumns().size() - 1) {
          sb.append(", ");
        }
      }

      sb.append("\nORDER BY ");
      for (int i = 0; i < tableMetaModel.getPartitionColumns().size(); i++) {
        ColumnMetaModel c = tableMetaModel.getPartitionColumns().get(i);
        sb.append("`").append(c.getColumnName()).append("`");
        if (i != tableMetaModel.getPartitionColumns().size() - 1) {
          sb.append(", ");
        }
      }

      sb.append("\nLIMIT")
        .append(" ")
        .append(tableMetaModel.getPartitions().size());
    }
    sb.append(";\n");

    return sb.toString();
  }

//  public static String getDDLSql(TableMetaModel tableMetaModel) {
//    return "SHOW CREATE TABLE "
//        + tableMetaModel.getDatabase()
//        + ".`" + tableMetaModel.getTable() + "`;\n";
//  }

//  public static String getDropViewStatement(String db, String tbl) {
//    return "DROP VIEW IF EXISTS " + db + ".`" + tbl + "`;\n";
//  }

//  public static String getCreateViewStatement(String db, String tbl, String viewText) {
//    return "CREATE VIEW IF NOT EXISTS " + db + ".`" + tbl + "` AS " + viewText + ";\n";
//  }

//  public static String getAddResourceStatement(
//      String resourceType, String filePath, String resourceName, String comment) {
//    String result = "ADD " + resourceType + " " + filePath + " AS " + resourceName;
//    if (!StringUtils.isNullOrEmpty(comment)) {
//      result = result + " COMMENT '" + comment + "'";
//    }
//    return result + ";\n";
//  }

//  public static String getCreateFunctionStatement(
//      String name, String classPath, List<String> resources) {
//    return "CREATE FUNCTION " + name + " as '" + classPath + "' USING '" +
//        String.join(",", resources) + "';\n";
//  }

  public static TableMetaModel getMcExternalTableMetaModel(
      TableMetaModel mcTableMetaModel,
      OssConfig ossConfig,
      String location,
      String rootJobId) {
    TableMetaModelBuilder builder = new TableMetaModelBuilder(mcTableMetaModel);
    return builder.table("temp_table_" + mcTableMetaModel.getTable() + "_by_mma_" + rootJobId)
                  .location(McSqlUtils.getMcExternalTableLocation(ossConfig, location))
                  .build();
  }

  private static String getMcExternalTableLocation(
      OssConfig ossConfig, String location) {
    // aliyun doc : https://help.aliyun.com/document_detail/72776.html
    // if use sts, Location format is:
    //    LOCATION 'oss://${endpoint}/${bucket}/${userfilePath}/'
    // else, Location format is:
    //    LOCATION 'oss://${accessKeyId}:${accessKeySecret}@${endpoint}/${bucket}/${userPath}/'
    if (StringUtils.isNullOrEmpty(ossConfig.getEndpointForMc())
        || StringUtils.isNullOrEmpty(ossConfig.getOssBucket())) {
      throw new IllegalArgumentException("Undefined OSS endpoint or OSS bucket");
    }
    String ossPrefix = OssConfig.PREFIX;

    StringBuilder locationBuilder = new StringBuilder(ossPrefix);

    if (StringUtils.isNullOrEmpty(ossConfig.getOssRoleArn())) {
      locationBuilder.append(ossConfig.getOssAccessId())
                     .append(":")
                     .append(ossConfig.getOssAccessKey()).append("@");
    }

    String ossEndpoint = ossConfig.getEndpointForMc();
    if (ossEndpoint.startsWith(ossPrefix)) {
      ossEndpoint = ossEndpoint.substring(ossPrefix.length());
    }
    String ossBucket = ossConfig.getOssBucket();
    String combinedPath = Paths.get(ossEndpoint, ossBucket, location).toString();
    locationBuilder.append(combinedPath);

    return locationBuilder.toString();
  }

//
//  public static String getOssTableDataPath(
//      String jobId,
//      String ossEndpoint,
//      String ossBucket,
//      String ossAccessKeyId,
//      String ossAccessKeySecret,
//      String ossRoleArn,
//      String catalogName,
//      String schemaName,
//      String tableName) {
//    // See : https://help.aliyun.com/document_detail/72776.html
//    // With sts, the location format is:
//    //    LOCATION 'oss://${endpoint}/${bucket}/${userfilePath}/'
//    // else, the location format is:
//    //    LOCATION 'oss://${accessKeyId}:${accessKeySecret}@${endpoint}/${bucket}/${userPath}/'
//
//    if (StringUtils.isNullOrEmpty(ossEndpoint) || StringUtils.isNullOrEmpty(ossBucket)) {
//      throw new IllegalArgumentException("Undefined OSS endpoint or OSS bucket");
//    }
//    String ossPrefix = "oss://";
//    StringBuilder locationBuilder = new StringBuilder(ossPrefix);
//    if (StringUtils.isNullOrEmpty(ossRoleArn)) {
//      locationBuilder.append(ossAccessKeyId)
//                     .append(":")
//                     .append(ossAccessKeySecret).append("@");
//    }
//    ossEndpoint = ossEndpoint.startsWith(ossPrefix) ?
//        ossEndpoint.substring(ossPrefix.length()) : ossEndpoint;
//    locationBuilder.append(ossEndpoint);
//    if (!ossEndpoint.endsWith("/")) {
//      locationBuilder.append("/");
//    }
//    locationBuilder.append(ossBucket);
//    if (!ossBucket.endsWith("/")) {
//      locationBuilder.append("/");
//    }
//    locationBuilder.append(jobId).append("/");
//    locationBuilder.append(catalogName).append("/");
////    locationBuilder.append(schemaName).append("/");
//    locationBuilder.append(tableName).append("/");
//    return locationBuilder.toString();
//  }

  private static String getPartitionSpec(
      List<ColumnMetaModel> partitionColumns, PartitionMetaModel partitionMetaModel) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = partitionMetaModel.getPartitionValues().get(i);

      sb.append(partitionColumn.getColumnName()).append("=");
      if ("STRING".equalsIgnoreCase(partitionColumn.getType())) {
        sb.append("'").append(partitionValue).append("'");
      } else if ("BIGINT".equalsIgnoreCase(partitionColumn.getType())
          || "INT".equalsIgnoreCase(partitionColumn.getType())
          || "SMALLINT".equalsIgnoreCase(partitionColumn.getType())
          || "TINYINT".equalsIgnoreCase(partitionColumn.getType())) {
        // Although the partition column type is integer, the partition values returned by HMS
        // client may have leading zeros. Let's say the partition in hive is hour=09. When the
        // partition is added in MC, the partition value will still be 09. In this example, however,
        // the UDTF will receive an integer 9 and creating an upload session with  9 will end up
        // with an error "No such partition". So, the leading zeros must be removed here.
        int count = 0;
        for (; count < partitionValue.length(); count++) {
          if ('0' != partitionValue.charAt(count)) {
            break;
          }
        }

        if (count != 0) {
          LOG.warn("Partition value: {}, leading zero count: {}", partitionValue, count);
        }

        if (count == partitionValue.length()) {
          sb.append("0");
        } else {
          sb.append(partitionValue, count, partitionValue.length());
        }
      } else {
        // TODO: handle __HIVE_DEFAULT_PARTITION__
        sb.append(partitionValue);
      }
      if (i != partitionColumns.size() - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }

  private static String getWhereCondition(TableMetaModel tableMetaModel) {
    if (tableMetaModel == null) {
      throw new IllegalArgumentException("'tableMetaModel' cannot be null");
    }

    StringBuilder sb = new StringBuilder();

    // Return if this is not a partitioned table
    if (tableMetaModel.getPartitionColumns().isEmpty()
        || tableMetaModel.getPartitions().isEmpty()) {
      return sb.toString();
    }

    sb.append("WHERE\n");
    for (int i = 0; i < tableMetaModel.getPartitions().size(); i++) {
      String entry = getWhereConditionEntry(
          tableMetaModel.getPartitionColumns(), tableMetaModel.getPartitions().get(i));
      sb.append(entry);

      if (i != tableMetaModel.getPartitions().size() - 1) {
        sb.append(" OR\n");
      }
    }
    return sb.toString();
  }

  private static String getWhereConditionEntry(
      List<ColumnMetaModel> partitionColumns,
      PartitionMetaModel partitionMetaModel) {
    if (partitionColumns == null || partitionMetaModel == null) {
      throw new IllegalArgumentException(
          "'partitionColumns' or 'partitionMetaModel' cannot be null");
    }

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = partitionMetaModel.getPartitionValues().get(i);

      sb.append(partitionColumn.getColumnName()).append("=");
      if ("STRING".equalsIgnoreCase(partitionColumn.getType())) {
        sb.append("'").append(partitionValue).append("'");
      } else {
        sb.append(partitionValue);
      }
      if (i != partitionColumns.size() - 1) {
        sb.append(" AND ");
      }
    }

    return sb.toString();
  }

  private static String getCoordinate(TableMetaModel tableMetaModel) {
    String schemaPart = "";
    if (tableMetaModel.getSchema() != null) {
      schemaPart = tableMetaModel.getSchema() + ".";
    }
    return tableMetaModel.getDatabase() + "." + schemaPart + "`" + tableMetaModel.getTable() + "`";
  }

}
