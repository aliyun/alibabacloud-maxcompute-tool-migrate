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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.Constants;
import com.aliyun.odps.mma.config.ExternalTableConfig;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.meta.MetaSource.ColumnMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.utils.StringUtils;

public class McSqlUtils {
  static final Logger LOG = LogManager.getLogger(McSqlUtils.class);

  public static String getDropTableStatement(String db, String tb) {
    return "DROP TABLE IF EXISTS " + db + ".`" + tb + "`;\n";
  }

  public static String getCreateTableStatement(TableMetaModel tableMetaModel) {
    return getCreateTableStatement(tableMetaModel, null);
  }

  public static String getCreateTableStatement(
      TableMetaModel tableMetaModel, ExternalTableConfig externalTableConfig) {
    StringBuilder sb = new StringBuilder();
    if (externalTableConfig != null) {
      sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
    } else {
      sb.append("CREATE TABLE IF NOT EXISTS ");
    }
    sb.append(tableMetaModel.getDatabase()).append(".")
      .append("`").append(tableMetaModel.getTable()).append("`");
    sb.append(getCreateTableStatementWithoutDatabaseName(tableMetaModel, externalTableConfig));
    return sb.toString();
  }

  private static String getCreateTableStatementWithoutDatabaseName(
      TableMetaModel tableMetaModel, ExternalTableConfig externalTableConfig) {
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

    if (externalTableConfig != null) {
      switch (externalTableConfig.getStorage()) {
        case OSS:
          sb.append(getCreateOssExternalTableCondition(tableMetaModel, externalTableConfig));
          break;
        default:
          throw new IllegalArgumentException("Unknown external table storage: " + externalTableConfig.getStorage().name());
      }
    }

    sb.append(";\n");

    return sb.toString();
  }

  private static String getCreateOssExternalTableCondition(
      TableMetaModel tableMetaModel, ExternalTableConfig externalTableConfig) {
    StringBuilder sb = new StringBuilder();
//    OssExternalTableConfig ossExternalTableConfig = (OssExternalTableConfig) externalTableConfig;
//
//    if (!StringUtils.isNullOrEmpty(ossExternalTableConfig.getRoleRan())) {
//      tableMetaModel.serDeProperties.put("odps.properties.rolearn",
//                                         ossExternalTableConfig.getRoleRan());
//    }
//
//    sb.append("ROW FORMAT serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n");
//    if (tableMetaModel.serDeProperties != null && !tableMetaModel.serDeProperties.isEmpty()) {
//      sb.append("WITH SERDEPROPERTIES (").append("\n");
//      List<String> propertyStrings = new LinkedList<>();
//      for (Entry<String, String> property : tableMetaModel.serDeProperties.entrySet()) {
//        String propertyString = String.format(
//            "'%s'='%s'",
//            StringEscapeUtils.escapeJava(property.getKey()),
//            StringEscapeUtils.escapeJava(property.getValue()));
//        propertyStrings.add(propertyString);
//      }
//      sb.append(String.join(",\n", propertyStrings)).append(")\n");
//    }

    // TODO: support other formats & compression methods
    sb.append("\nSTORED AS ORC")
      .append("\nLOCATION '").append(externalTableConfig.getLocation()).append("'");
//      .append("\nTBLPROPERTIES (")
//      .append("\n'mcfed.mapreduce.output.fileoutputformat.compress'='true',")
//      .append("\n'mcfed.mapreduce.output.fileoutputformat.compress.codec'='com.hadoop.compression.lzo.LzoCodec');");
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
    sb.append(tableMetaModel.getDatabase())
      .append(".`").append(tableMetaModel.getTable()).append("`\n");
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
    sb.append(tableMetaModel.getDatabase())
      .append(".`").append(tableMetaModel.getTable()).append("`\n");
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
    sb.append("INSERT OVERWRITE TABLE ")
      .append(dest.getDatabase())
      .append(".`").append(dest.getTable()).append("`\n");
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
    sb.append("SELECT * FROM ")
      .append(source.getDatabase())
      .append(".`").append(source.getTable()).append("`").append("\n");
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
    String database = tableMetaModel.getDatabase();
    String table = tableMetaModel.getTable();
    sb.append(database).append(".`").append(table).append("`\n");

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

  public static String getOssTablePath(
      OssConfig ossConfig, String ossFilePath) {
    // aliyun doc : https://help.aliyun.com/document_detail/72776.html
    // if use sts, Location format is:
    //    LOCATION 'oss://${endpoint}/${bucket}/${userfilePath}/'
    // else, Location format is:
    //    LOCATION 'oss://${accessKeyId}:${accessKeySecret}@${endpoint}/${bucket}/${userPath}/'
    if (StringUtils.isNullOrEmpty(ossConfig.getOssEndpoint())
        || StringUtils.isNullOrEmpty(ossConfig.getOssBucket())) {
      throw new IllegalArgumentException("Undefined OSS endpoint or OSS bucket");
    }
    String ossPrefix = "oss://";
    StringBuilder locationBuilder = new StringBuilder(ossPrefix);
    if (StringUtils.isNullOrEmpty(ossConfig.getOssRoleArn())) {
      locationBuilder.append(ossConfig.getOssAccessId())
                     .append(":")
                     .append(ossConfig.getOssAccessKey()).append("@");
    }
    String ossEndpoint = ossConfig.getOssEndpoint().startsWith(ossPrefix) ?
        ossConfig.getOssEndpoint().substring(ossPrefix.length()) : ossConfig.getOssEndpoint();
    String ossBucket = ossConfig.getOssBucket();
    locationBuilder.append(ossEndpoint);
    if (!ossEndpoint.endsWith("/")) {
      locationBuilder.append("/");
    }
    locationBuilder.append(ossBucket);
    if (!ossBucket.endsWith("/")) {
      locationBuilder.append("/");
    }
    if (ossFilePath.startsWith("/")) {
      locationBuilder.append(ossFilePath.substring(1));
    } else {
      locationBuilder.append(ossFilePath);
    }
    if (!ossFilePath.endsWith("/")) {
      locationBuilder.append("/");
    }
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
}
