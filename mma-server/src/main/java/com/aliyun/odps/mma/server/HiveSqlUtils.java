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

package com.aliyun.odps.mma.server;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.mma.meta.MetaSource.ColumnMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class HiveSqlUtils {

  public static String getUdtfSql(
      String mcEndpoint,
      String mcBearerToken,
      TableMetaModel hiveTableMetaModel,
      TableMetaModel mcTableMetaModel) {
    StringBuilder sb = new StringBuilder();

    List<String> hiveColumnNames = new ArrayList<>();
    List<String> mcColumnNames = new ArrayList<>();
    for (ColumnMetaModel hiveColumnMetaModel : hiveTableMetaModel.getColumns()) {
      hiveColumnNames.add(hiveColumnMetaModel.getColumnName());
    }
    for (ColumnMetaModel mcColumnMetaModel : mcTableMetaModel.getColumns()) {
      mcColumnNames.add(mcColumnMetaModel.getColumnName());
    }

    List<String> mcPartitionColumnNames = new ArrayList<>();
    for (ColumnMetaModel mcPartitionColumnMetaModel : mcTableMetaModel.getPartitionColumns()) {
      mcPartitionColumnNames.add(mcPartitionColumnMetaModel.getColumnName());

    }
    for (ColumnMetaModel hivePartitionColumnMetaModel : hiveTableMetaModel.getPartitionColumns()) {
      hiveColumnNames.add(hivePartitionColumnMetaModel.getColumnName());
    }

    sb.append("SELECT default.odps_data_dump_multi (\n")
      .append("'").append(mcBearerToken).append("',\n")
      .append("'").append(mcEndpoint).append("',\n")
      .append("'").append(mcTableMetaModel.getDatabase()).append("',\n")
      .append("'").append(mcTableMetaModel.getTable()).append("',\n")
      .append("'").append(String.join(",", mcColumnNames)).append("',\n")
      .append("'").append(String.join(",", mcPartitionColumnNames)).append("',\n");

    for (int i = 0; i < hiveColumnNames.size(); i++) {
      sb.append("`").append(hiveColumnNames.get(i)).append("`");
      if (i != hiveColumnNames.size() - 1) {
        sb.append(",\n");
      } else {
        sb.append(")\n");
      }
    }

    sb.append("FROM ")
      .append(hiveTableMetaModel.getDatabase())
      .append(".`")
      .append(hiveTableMetaModel.getTable())
      .append("`")
      .append("\n");
    String whereCondition = getWhereCondition(hiveTableMetaModel);
    sb.append(whereCondition);
    return sb.toString();
  }

  public static String getVerifySql(TableMetaModel hiveTableMetaModel) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");

    if (!hiveTableMetaModel.getPartitionColumns().isEmpty()) {
      for (int i = 0; i < hiveTableMetaModel.getPartitionColumns().size(); i++) {
        ColumnMetaModel c = hiveTableMetaModel.getPartitionColumns().get(i);
        sb.append("`").append(c.getColumnName()).append("`");
        sb.append(", ");
      }
    }

    sb.append("COUNT(1) FROM\n");
    sb.append(hiveTableMetaModel.getDatabase())
      .append(".`").append(hiveTableMetaModel.getTable()).append("`\n");

    if (!hiveTableMetaModel.getPartitionColumns().isEmpty()) {
      // Add where condition
      String whereCondition = getWhereCondition(hiveTableMetaModel);
      sb.append(whereCondition);

      sb.append("\nGROUP BY ");
      for (int i = 0; i < hiveTableMetaModel.getPartitionColumns().size(); i++) {
        ColumnMetaModel c = hiveTableMetaModel.getPartitionColumns().get(i);
        sb.append("`").append(c.getColumnName()).append("`");
        if (i != hiveTableMetaModel.getPartitionColumns().size() - 1) {
          sb.append(", ");
        }
      }

      sb.append("\nORDER BY ");
      for (int i = 0; i < hiveTableMetaModel.getPartitionColumns().size(); i++) {
        ColumnMetaModel c = hiveTableMetaModel.getPartitionColumns().get(i);
        sb.append("`").append(c.getColumnName()).append("`");
        if (i != hiveTableMetaModel.getPartitionColumns().size() - 1) {
          sb.append(", ");
        }
      }
    }
    sb.append("\n");
    return sb.toString();
  }

  private static String getWhereCondition(TableMetaModel hiveTableMetaModel) {
    StringBuilder sb = new StringBuilder();

    // Return if this is not a partitioned table
    if (hiveTableMetaModel.getPartitionColumns().isEmpty()
        || hiveTableMetaModel.getPartitions().isEmpty()) {
      return sb.toString();
    }

    sb.append("WHERE\n");
    for (int i = 0; i < hiveTableMetaModel.getPartitions().size(); i++) {
      String entry = getWhereConditionEntry(
          hiveTableMetaModel.getPartitionColumns(), hiveTableMetaModel.getPartitions().get(i));
      sb.append(entry);

      if (i != hiveTableMetaModel.getPartitions().size() - 1) {
        sb.append(" OR\n");
      }
    }

    sb.append("\n");
    return sb.toString();
  }

  private static String getWhereConditionEntry(
      List<ColumnMetaModel> partitionColumns,
      PartitionMetaModel partitionMetaModel) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = partitionMetaModel.getPartitionValues().get(i);

      sb.append(partitionColumn.getColumnName())
        .append("=")
        .append("cast('").append(partitionValue)
        .append("' AS ")
        .append(partitionColumn.getType()).append(")");

      if (i != partitionColumns.size() - 1) {
        sb.append(" AND ");
      }
    }

    return sb.toString();
  }
}
