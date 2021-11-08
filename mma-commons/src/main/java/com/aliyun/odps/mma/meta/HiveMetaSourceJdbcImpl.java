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

package com.aliyun.odps.mma.meta;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.meta.model.ColumnMetaModel;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel.TableMetaModelBuilder;

public class HiveMetaSourceJdbcImpl extends TimedMetaSource {

  private static final Logger LOG = LogManager.getLogger(HiveMetaSourceJdbcImpl.class);

  /**
   * Limit the number of available to protect the Hiveserver2 from being overwhelmed
   */
  private static final int CONNECTION_COUNT_MAX = 15;
  private final Semaphore available = new Semaphore(CONNECTION_COUNT_MAX, true);

  private String hiveJdbcUrl;
  private String username;
  private String password;

  public HiveMetaSourceJdbcImpl(
      String hiveJdbcUrl,
      String username,
      String password,
      Map<String, String> javaSecurityConfigs) throws ClassNotFoundException {
    this.hiveJdbcUrl = Validate.notBlank(hiveJdbcUrl, "Hive JDBC URL cannot be null");
    this.username = Validate.notBlank(username, "Hive JDBC username cannot be null");
    this.password = password;

    LOG.info("Initializing HiveMetaSourceJdbcImpl, JDBC URL: {}, username: {}, system properties: {}",
             hiveJdbcUrl,
             username,
             javaSecurityConfigs);

    if (javaSecurityConfigs != null && javaSecurityConfigs.size() > 0) {
      for (Entry<String, String> entry : javaSecurityConfigs.entrySet()) {
        LOG.info("Set system property {} = {}", entry.getKey(), entry.getValue());
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }

    Class.forName("org.apache.hive.jdbc.HiveDriver");
    LOG.info("HiveMetaSourceJdbcImpl initialized");
  }

  private Connection getConnection() throws SQLException, InterruptedException {
    available.acquire();
    LOG.debug(
        "Get connection, max connection: {}, current: {}",
        CONNECTION_COUNT_MAX,
        CONNECTION_COUNT_MAX - available.availablePermits());
    return DriverManager.getConnection(hiveJdbcUrl, username, password);
  }

  private void releaseConnection() {
    LOG.debug(
        "Release connection, max connection: {}, current: {}",
        CONNECTION_COUNT_MAX,
        CONNECTION_COUNT_MAX - available.availablePermits());
    available.release();
  }

  @Override
  public TableMetaModel timedGetTableMeta(String databaseName, String tableName) throws Exception {
    return getTableMetaInternal(databaseName, tableName, false);
  }

  @Override
  public TableMetaModel timedGetTableMetaWithoutPartitionMeta(String databaseName, String tableName)
      throws Exception {
    return getTableMetaInternal(databaseName, tableName, true);
  }

  private TableMetaModel getTableMetaInternal(
      String databaseName,
      String tableName,
      boolean withoutPartitionMeta) throws SQLException, InterruptedException {
    List<ColumnMetaModel> columns = new LinkedList<>();
    List<ColumnMetaModel> partitionColumns = new LinkedList<>();
    List<PartitionMetaModel> partitions = new LinkedList<>();
    Long size = null;
    Long creationTime = null;
    Long lastModificationTime = null;
    String location = null;
    String inputFormat = null;
    String outputFormat = null;
    String serDe = null;
    try (Connection connection = getConnection()) {
      try (Statement stmt = connection.createStatement()) {
        // Example:
        //  +-------------------------------+----------------------------------------------------+-----------------------------+
        //  |           col_name            |                     data_type                      |           comment           |
        //  +-------------------------------+----------------------------------------------------+-----------------------------+
        //  | # col_name                    | data_type                                          | comment                     |
        //  | t_tinyint                     | tinyint                                            |                             |
        //  | t_smallint                    | smallint                                           |                             |
        //  | t_int                         | int                                                |                             |
        //  | t_bigint                      | bigint                                             |                             |
        //  | t_float                       | float                                              |                             |
        //  | t_double                      | double                                             |                             |
        //  | t_decimal                     | decimal(10,0)                                      |                             |
        //  | t_timestamp                   | timestamp                                          |                             |
        //  | t_string                      | string                                             |                             |
        //  | t_varchar                     | varchar(255)                                       |                             |
        //  | t_char                        | char(255)                                          |                             |
        //  | t_boolean                     | boolean                                            |                             |
        //  | t_binary                      | binary                                             |                             |
        //  | t_array                       | array<string>                                      |                             |
        //  | t_map                         | map<string,string>                                 |                             |
        //  | t_struct                      | struct<c1:string,c2:bigint>                        |                             |
        //  |                               | NULL                                               | NULL                        |
        //  | # Partition Information       | NULL                                               | NULL                        |
        //  | # col_name                    | data_type                                          | comment                     |
        //  | p1                            | string                                             |                             |
        //  | p2                            | bigint                                             |                             |
        //  |                               | NULL                                               | NULL                        |
        //  | # Detailed Table Information  | NULL                                               | NULL                        |
        //  | Database:                     | mma_test                                           | NULL                        |
        //  | OwnerType:                    | USER                                               | NULL                        |
        //  | Owner:                        | hive                                               | NULL                        |
        //  | CreateTime:                   | Tue Apr 06 17:56:20 CST 2021                       | NULL                        |
        //  | LastAccessTime:               | UNKNOWN                                            | NULL                        |
        //  | Retention:                    | 0                                                  | NULL                        |
        //  | Location:                     | hdfs://emr-cluster/user/hive/warehouse/mma_test.db/test_text_partitioned_10x1k | NULL                        |
        //  | Table Type:                   | MANAGED_TABLE                                      | NULL                        |
        //  | Table Parameters:             | NULL                                               | NULL                        |
        //  |                               | COLUMN_STATS_ACCURATE                              | {\"BASIC_STATS\":\"true\"}  |
        //  |                               | bucketing_version                                  | 2                           |
        //  |                               | numFiles                                           | 11                          |
        //  |                               | numPartitions                                      | 11                          |
        //  |                               | numRows                                            | 11000                       |
        //  |                               | rawDataSize                                        | 24896492                    |
        //  |                               | totalSize                                          | 24907492                    |
        //  |                               | transient_lastDdlTime                              | 1617702980                  |
        //  |                               | NULL                                               | NULL                        |
        //  | # Storage Information         | NULL                                               | NULL                        |
        //  | SerDe Library:                | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe | NULL                        |
        //  | InputFormat:                  | org.apache.hadoop.mapred.TextInputFormat           | NULL                        |
        //  | OutputFormat:                 | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat | NULL                        |
        //  | Compressed:                   | No                                                 | NULL                        |
        //  | Num Buckets:                  | -1                                                 | NULL                        |
        //  | Bucket Columns:               | []                                                 | NULL                        |
        //  | Sort Columns:                 | []                                                 | NULL                        |
        //  | Storage Desc Params:          | NULL                                               | NULL                        |
        //  |                               | serialization.format                               | 1                           |
        //  +-------------------------------+----------------------------------------------------+-----------------------------+
        String query = "DESC FORMATTED " + databaseName + ".`" + tableName + "`";
        LOG.debug("Execute query: {}", query);
        try (ResultSet rs = stmt.executeQuery(query)) {
          boolean isPartitionColumn = false;
          boolean isDetailedTableInfo = false;
          boolean isStorageInformation = false;
          boolean isTableParameter = false;
          while (rs.next()) {
            String field1 = StringUtils.trim(rs.getString(1));
            String field2 = StringUtils.trim(rs.getString(2));
            String field3 = StringUtils.trim(rs.getString(3));
            if (StringUtils.isBlank(field1)
                && StringUtils.isBlank(field2)
                && StringUtils.isBlank(field3)) {
              // Skip
            } else if ("# col_name".equals(field1)) {
              // Skip
            } else if ("# Partition Information".equals(field1)) {
              isPartitionColumn = true;
              isDetailedTableInfo = false;
              isStorageInformation = false;
            } else if ("# Detailed Table Information".equals(field1)) {
              isPartitionColumn = false;
              isDetailedTableInfo = true;
              isStorageInformation = false;
            } else if ("# Storage Information".equals(field1)) {
              isPartitionColumn = false;
              isDetailedTableInfo = false;
              isStorageInformation = true;
            } else if (isPartitionColumn) {
              // Handle partition columns
              String partitionColumnName = Validate.notNull(
                  field1, "Partition column name is null");
              String type = Validate.notNull(
                  field2, "Type is null");
              String comment = field3;
              ColumnMetaModel columnMetaModel = new ColumnMetaModel(
                  partitionColumnName,
                  type,
                  comment);
              partitionColumns.add(columnMetaModel);
            } else if (isDetailedTableInfo) {
              if ("Table Parameters:".equals(field1)) {
                isTableParameter = true;
              }
              if ("Location:".equals(field1)) {
                location = field2;
              }
              if (isTableParameter && "totalSize".equals(field2)) {
                size = Long.valueOf(Validate.notNull(
                    field3, "Table size is null").trim());
              } else if (isTableParameter && "transient_lastDdlTime".equals(field2)) {
                lastModificationTime = Long.valueOf(Validate.notNull(
                    field3, "Table mtime is null").trim());
              }
            } else if (isStorageInformation) {
              // Skip
            } else {
              // Handle columns
              String columnName = Validate.notNull(
                  field1, "Column name is null");
              String type = Validate.notNull(
                  field2, "Type is null");
              String comment = field3;
              ColumnMetaModel columnMetaModel = new ColumnMetaModel(columnName, type, comment);
              columns.add(columnMetaModel);
            }
          }
        }
      }

      if (!withoutPartitionMeta && !partitionColumns.isEmpty()) {
        List<List<String>> partitionValuesList =
            listPartitionsInternal(connection, databaseName, tableName);
        List<String> partitionColumnNames = partitionColumns
            .stream()
            .map(ColumnMetaModel::getColumnName)
            .collect(Collectors.toList());
        for (List<String> partitionValues : partitionValuesList) {
          partitions.add(
              getPartitionMetaInternal(
                  connection,
                  databaseName,
                  tableName,
                  partitionColumnNames,
                  partitionValues));
        }
      }

      TableMetaModelBuilder builder = new TableMetaModelBuilder(databaseName, tableName, columns);
      builder.partitionColumns(partitionColumns);
      builder.size(size);
      builder.creationTime(creationTime);
      builder.lastModificationTime(lastModificationTime);
      builder.location(location);
      builder.inputFormat(inputFormat);
      builder.outputFormat(outputFormat);
      builder.serDe(serDe);
      builder.partitions(partitions);
      return builder.build();
    } finally {
      releaseConnection();
    }
  }

  private static PartitionMetaModel getPartitionMetaInternal(
      Connection connection,
      String databaseName,
      String tableName,
      List<String> partitionColumnNames,
      List<String> partitionValues) throws SQLException {
    String partitionSpec = getPartitionSpec(partitionColumnNames, partitionValues, ",");
    String location = null;
    Long creationTime = null;
    Long lastModificationTime = null;
    Long size = null;
    try (Statement stmt = connection.createStatement()) {
      // Example:
      //  +-----------------------------------+----------------------------------------------------+-----------------------------+
      //  |             col_name              |                     data_type                      |           comment           |
      //  +-----------------------------------+----------------------------------------------------+-----------------------------+
      //  | # col_name                        | data_type                                          | comment                     |
      //  | t_tinyint                         | tinyint                                            |                             |
      //  | t_smallint                        | smallint                                           |                             |
      //  | t_int                             | int                                                |                             |
      //  | t_bigint                          | bigint                                             |                             |
      //  | t_float                           | float                                              |                             |
      //  | t_double                          | double                                             |                             |
      //  | t_decimal                         | decimal(10,0)                                      |                             |
      //  | t_timestamp                       | timestamp                                          |                             |
      //  | t_string                          | string                                             |                             |
      //  | t_varchar                         | varchar(255)                                       |                             |
      //  | t_char                            | char(255)                                          |                             |
      //  | t_boolean                         | boolean                                            |                             |
      //  | t_binary                          | binary                                             |                             |
      //  | t_array                           | array<string>                                      |                             |
      //  | t_map                             | map<string,string>                                 |                             |
      //  | t_struct                          | struct<c1:string,c2:bigint>                        |                             |
      //  |                                   | NULL                                               | NULL                        |
      //  | # Partition Information           | NULL                                               | NULL                        |
      //  | # col_name                        | data_type                                          | comment                     |
      //  | p1                                | string                                             |                             |
      //  | p2                                | bigint                                             |                             |
      //  |                                   | NULL                                               | NULL                        |
      //  | # Detailed Partition Information  | NULL                                               | NULL                        |
      //  | Partition Value:                  | [mma_test, 123456]                                 | NULL                        |
      //  | Database:                         | mma_test                                           | NULL                        |
      //  | Table:                            | test_text_partitioned_10x1k                        | NULL                        |
      //  | CreateTime:                       | Tue Apr 06 18:20:29 CST 2021                       | NULL                        |
      //  | LastAccessTime:                   | UNKNOWN                                            | NULL                        |
      //  | Location:                         | hdfs://emr-cluster/user/hive/warehouse/mma_test.db/test_text_partitioned_10x1k/p1=mma_test/p2=123456 | NULL                        |
      //  | Partition Parameters:             | NULL                                               | NULL                        |
      //  |                                   | COLUMN_STATS_ACCURATE                              | {\"BASIC_STATS\":\"true\"}  |
      //  |                                   | numFiles                                           | 1                           |
      //  |                                   | numRows                                            | 1000                        |
      //  |                                   | rawDataSize                                        | 2263259                     |
      //  |                                   | totalSize                                          | 2264259                     |
      //  |                                   | transient_lastDdlTime                              | 1617704429                  |
      //  |                                   | NULL                                               | NULL                        |
      //  | # Storage Information             | NULL                                               | NULL                        |
      //  | SerDe Library:                    | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe | NULL                        |
      //  | InputFormat:                      | org.apache.hadoop.mapred.TextInputFormat           | NULL                        |
      //  | OutputFormat:                     | org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat | NULL                        |
      //  | Compressed:                       | No                                                 | NULL                        |
      //  | Num Buckets:                      | -1                                                 | NULL                        |
      //  | Bucket Columns:                   | []                                                 | NULL                        |
      //  | Sort Columns:                     | []                                                 | NULL                        |
      //  | Storage Desc Params:              | NULL                                               | NULL                        |
      //  |                                   | serialization.format                               | 1                           |
      //  +-----------------------------------+----------------------------------------------------+-----------------------------+
      String query = "DESC FORMATTED "
          + databaseName + ".`" + tableName + "` PARTITION(" + partitionSpec + ")";
      LOG.debug("Execute query: {}", query);
      try (ResultSet rs = stmt.executeQuery(query)) {
        boolean isPartitionColumn = false;
        boolean isDetailedPartitionInfo = false;
        boolean isStorageInformation = false;
        boolean isPartitionParameter = false;
        while (rs.next()) {
          String field1 = StringUtils.trim(rs.getString(1));
          String field2 = StringUtils.trim(rs.getString(2));
          String field3 = StringUtils.trim(rs.getString(3));
          if (StringUtils.isBlank(field1)
              && StringUtils.isBlank(field2)
              && StringUtils.isBlank(field3)) {
            // Skip
          } else if ("# col_name".equals(field1)) {
            // Skip
          } else if ("# Partition Information".equals(field1)) {
            isPartitionColumn = true;
            isDetailedPartitionInfo = false;
            isStorageInformation = false;
          } else if ("# Detailed Partition Information".equals(field1)) {
            isPartitionColumn = false;
            isDetailedPartitionInfo = true;
            isStorageInformation = false;
          } else if ("# Storage Information".equals(field1)) {
            isPartitionColumn = false;
            isDetailedPartitionInfo = false;
            isStorageInformation = true;
          } else if (isPartitionColumn) {
            // Skip
          } else if (isDetailedPartitionInfo) {
            if ("Partition Parameters:".equals(field1)) {
              isPartitionParameter = true;
            }
            if (isPartitionParameter && "totalSize".equals(field2)) {
              size = Long.valueOf(Validate.notNull(
                  field3, "Partition size is null").trim());
            }
            if (isPartitionParameter && "transient_lastDdlTime".equals(field2)) {
              lastModificationTime = Long.valueOf(Validate.notNull(
                  field3, "Partition mtime is null").trim());
            }
            if ("Location:".equals(field1)) {
              location = field2;
            }
          } else if (isStorageInformation) {
            // Skip
          } else {
            // Handle columns
            // Skip
          }
        }
      }

      return new PartitionMetaModel(
          partitionValues,
          location,
          creationTime,
          lastModificationTime,
          size);
    }
  }

  @Override
  public PartitionMetaModel timedGetPartitionMeta(
      String databaseName,
      String tableName,
      List<String> partitionValues) throws Exception {
    try (Connection connection = getConnection()) {
      List<String> partitionColumnNames = getPartitionColumnNames(
          connection,
          databaseName,
          tableName);
      return getPartitionMetaInternal(
          connection,
          databaseName,
          tableName,
          partitionColumnNames,
          partitionValues);
    } finally {
      releaseConnection();
    }
  }

  @Override
  public List<String> listResources(String databaseName) throws Exception {
    throw new MmaException("list hive resources not supported");
  }

  @Override
  public List<String> listFunctions(String databaseName) throws Exception {
    throw new MmaException("list hive functions not supported");
  }

  @Override
  public ResourceMetaModel getResourceMeta(String databaseName, String resourceName)
      throws Exception {
    throw new MmaException("get hive resource not supported");
  }

  @Override
  public FunctionMetaModel getFunctionMeta(String databaseName, String functionName)
      throws Exception {
    throw new MmaException("get hive function not supported");
  }

  @Override
  public List<ObjectType> getSupportedObjectTypes() {
    return Collections.singletonList(ObjectType.TABLE);
  }

  @Override
  public boolean timedHasDatabase(String databaseName) throws Exception {
    try (Connection connection = getConnection()) {
      DatabaseMetaData meta = connection.getMetaData();
      try (ResultSet rs = meta.getSchemas(null, databaseName)) {
        while (rs.next()) {
          if (databaseName.equalsIgnoreCase(rs.getString("TABLE_SCHEM"))) {
            return true;
          }
        }
      }
    } finally {
      releaseConnection();
    }
    return false;
  }

  @Override
  public boolean timedHasTable(String databaseName, String tableName) throws Exception {
    try (Connection connection = getConnection()) {
      DatabaseMetaData meta = connection.getMetaData();
      try (ResultSet rs = meta.getTables(null, databaseName, tableName, null)) {
        while (rs.next()) {
          if (databaseName.equalsIgnoreCase(rs.getString("TABLE_SCHEM"))
              && tableName.equalsIgnoreCase(rs.getString("TABLE_NAME"))) {
            return true;
          }
        }
      }
    } finally {
      releaseConnection();
    }
    return false;
  }

  @Override
  public boolean timedHasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception {
    try (Connection connection = getConnection()) {
      List<String> partitionColumnNames =
          getPartitionColumnNames(connection, databaseName, tableName);

      String partitionSpec = getPartitionSpec(partitionColumnNames, partitionValues, ",");

      // Check if the partition exists
      try (Statement stmt = connection.createStatement()) {
        String query =
            "DESC " + databaseName + ".`" + tableName + "` PARTITION(" + partitionSpec + ")";
        LOG.debug("Execute query: {}", query);
        try (ResultSet rs = stmt.executeQuery(query)) {
          while (rs.next()) {
            return true;
          }
        }
      }
    } finally {
      releaseConnection();
    }

    return false;
  }

  private List<String> getPartitionColumnNames(
      Connection connection,
      String databaseName,
      String tableName) throws SQLException {
    List<String> partitionColumnNames = new LinkedList<>();
    try (Statement stmt = connection.createStatement()) {
      // Get partition column names
      // Example:
      //  +--------------------------+------------------------------+----------+
      //  |         col_name         |          data_type           | comment  |
      //  +--------------------------+------------------------------+----------+
      //  | t_tinyint                | tinyint                      |          |
      //  | t_smallint               | smallint                     |          |
      //  | t_int                    | int                          |          |
      //  | t_bigint                 | bigint                       |          |
      //  | t_float                  | float                        |          |
      //  | t_double                 | double                       |          |
      //  | t_decimal                | decimal(10,0)                |          |
      //  | t_timestamp              | timestamp                    |          |
      //  | t_string                 | string                       |          |
      //  | t_varchar                | varchar(255)                 |          |
      //  | t_char                   | char(255)                    |          |
      //  | t_boolean                | boolean                      |          |
      //  | t_binary                 | binary                       |          |
      //  | t_array                  | array<string>                |          |
      //  | t_map                    | map<string,string>           |          |
      //  | t_struct                 | struct<c1:string,c2:bigint>  |          |
      //  | p1                       | string                       |          |
      //  | p2                       | bigint                       |          |
      //  |                          | NULL                         | NULL     |
      //  | # Partition Information  | NULL                         | NULL     |
      //  | # col_name               | data_type                    | comment  |
      //  | p1                       | string                       |          |
      //  | p2                       | bigint                       |          |
      //  +--------------------------+------------------------------+----------+
      boolean isPartitionColumn = false;
      String query = "DESC " + databaseName + ".`" + tableName +"`";
      LOG.debug("Execute query: {}", query);
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          String field1 = StringUtils.trim(rs.getString(1));
          String field2 = StringUtils.trim(rs.getString(2));
          String field3 = StringUtils.trim(rs.getString(3));
          if (StringUtils.isBlank(field1)
              && StringUtils.isBlank(field2)
              && StringUtils.isBlank(field3)) {
            // Skip
          } else if ("# Partition Information".equals(field1)) {
            isPartitionColumn = true;
          } else if ("# col_name".equals(field1)) {
            // Skip
          } else if (isPartitionColumn) {
            String partitionColumnName = Validate.notNull(
                field1, "Partition column name is null");
            partitionColumnNames.add(partitionColumnName);
          }
        }
      }
    }
    return partitionColumnNames;
  }

  private static String getPartitionSpec(
      List<String> partitionColumnNames,
      List<String> partitionValues,
      String separator) {
    if (partitionColumnNames.size() != partitionValues.size()) {
      throw new IllegalStateException(
          "The number of partition values and columns does not match. "
              + partitionValues.size() + " partition values. "
              + partitionColumnNames.size() + " partition columns.");
    }

    StringBuilder partitionSpecBuilder = new StringBuilder();
    for (int i = 0; i < partitionValues.size(); i++) {
      partitionSpecBuilder
          .append(partitionColumnNames.get(i))
          .append("='")
          .append(partitionValues.get(i))
          .append("'");
      if (i != partitionValues.size() - 1) {
        partitionSpecBuilder.append(separator);
      }
    }
    return partitionSpecBuilder.toString();
  }

  @Override
  public List<String> timedListDatabases() throws Exception {
    List<String> databaseNames = new LinkedList<>();
    try (Connection connection = getConnection()) {
      DatabaseMetaData meta = connection.getMetaData();
      try (ResultSet rs = meta.getSchemas()) {
        while (rs.next()) {
          databaseNames.add(rs.getString("TABLE_SCHEM"));
        }
      }
    } finally {
      releaseConnection();
    }
    return databaseNames;
  }

  @Override
  public List<String> timedListTables(String databaseName) throws Exception {
    List<String> tableNames = new LinkedList<>();
    try (Connection connection = getConnection()) {
      DatabaseMetaData meta = connection.getMetaData();
      try (ResultSet rs = meta.getTables(null, databaseName, null, null)) {
        while (rs.next()) {
          tableNames.add(rs.getString("TABLE_NAME"));
        }
      }
    } finally {
      releaseConnection();
    }
    return tableNames;
  }

  @Override
  public List<List<String>> timedListPartitions(String databaseName, String tableName) throws Exception {
    List<List<String>> partitionValuesList;
    try (Connection connection = getConnection()) {
      partitionValuesList = listPartitionsInternal(connection, databaseName, tableName);
    } finally {
      releaseConnection();
    }
    return partitionValuesList;
  }

  private static List<List<String>> listPartitionsInternal(
      Connection connection,
      String databaseName,
      String tableName) throws SQLException {
    List<List<String>> partitionValuesList = new LinkedList<>();
    try (Statement stmt = connection.createStatement()) {
      String query = "SHOW PARTITIONS " + databaseName + ".`" + tableName +"`";
      LOG.debug("Execute query: {}", query);
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          String field1 = StringUtils.trim(rs.getString(1));
          partitionValuesList.add(getPartitionValuesFromPartitionSpec(field1, "/"));
        }
      }
    }
    return partitionValuesList;
  }

  private static List<String> getPartitionValuesFromPartitionSpec(
      String partitionSpec, String separator) {
    // Parse SLASH separated partition spec and return a list of the partition values
    // Example: p1=JVioJ/p2=3140
    List<String> partitionValues = new LinkedList<>();
    String[] entries = partitionSpec.split(separator);
    for (String entry : entries) {
      partitionValues.add(entry.split("=")[1]);
    }
    return partitionValues;
  }

  @Override
  public void shutdown() {
  }
}
