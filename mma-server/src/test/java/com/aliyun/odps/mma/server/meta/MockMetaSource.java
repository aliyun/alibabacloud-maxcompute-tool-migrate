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

package com.aliyun.odps.mma.server.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.model.ColumnMetaModel;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel.TableMetaModelBuilder;

/**
 * Can be used as a mock MaxCompute or Hive meta source.
 */
public class MockMetaSource implements MetaSource {

  public static final String DB_NAME = "test";
  public static final String TBL_NON_PARTITIONED = "test_non_partitioned";
  public static final String TBL_PARTITIONED = "test_partitioned";

  private static TableMetaModel TBL_NON_PARTITIONED_META;
  static {
    List<ColumnMetaModel> columnMetaModels = new ArrayList<>(2);
    columnMetaModels.add(new ColumnMetaModel("foo", "STRING", "comment"));
    columnMetaModels.add(new ColumnMetaModel("bar", "BIGINT", "comment"));
    TableMetaModelBuilder builder = new TableMetaModelBuilder(
        DB_NAME,
        TBL_NON_PARTITIONED,
        columnMetaModels);
    builder.lastModificationTime(0L);
    TBL_NON_PARTITIONED_META = builder.build();
  }

  private static TableMetaModel TBL_PARTITIONED_META;
  static {
    List<ColumnMetaModel> columnMetaModels = new ArrayList<>(2);
    columnMetaModels.add(new ColumnMetaModel("foo", "STRING", "comment"));
    columnMetaModels.add(new ColumnMetaModel("bar", "BIGINT", "comment"));
    TableMetaModelBuilder builder = new TableMetaModelBuilder(
        DB_NAME,
        TBL_PARTITIONED,
        columnMetaModels);
    List<ColumnMetaModel> partitionColumnMetaModels = new ArrayList<>(2);
    partitionColumnMetaModels.add(
        new ColumnMetaModel("p1", "STRING", "first partition column"));
    partitionColumnMetaModels.add(
        new ColumnMetaModel("p2", "BIGINT", "second partition column"));
    builder.partitionColumns(partitionColumnMetaModels);
    builder.lastModificationTime(0L);

    List<PartitionMetaModel> partitionMetaModels = new ArrayList<>(2);
    List<String> partitionValues = new ArrayList<>(2);
    partitionValues.add("hello");
    partitionValues.add("1");
    partitionMetaModels.add(
        new PartitionMetaModel(
            partitionValues,
            "/test/test_partitioned/hello/1",
            0L,
            0L,
            1000000000L)
    );
    partitionValues = new ArrayList<>(2);
    partitionValues.add("world");
    partitionValues.add("2");
    partitionMetaModels.add(
        new PartitionMetaModel(
            partitionValues,
            "/test/test_partitioned/world/2",
            0L,
            0L,
            1000000000L)
    );
    builder.partitions(partitionMetaModels);
    TBL_PARTITIONED_META = builder.build();
  }

  public static final Map<String, TableMetaModel> TBL_NAME_2_TBL_META = new HashMap<>();
  static {
    TBL_NAME_2_TBL_META.put(TBL_NON_PARTITIONED, TBL_NON_PARTITIONED_META);
    TBL_NAME_2_TBL_META.put(TBL_PARTITIONED, TBL_PARTITIONED_META);
  }

  @Override
  public boolean hasDatabase(String databaseName) {
    return DB_NAME.equalsIgnoreCase(databaseName);
  }

  @Override
  public boolean hasTable(String databaseName, String tableName) {
    return DB_NAME.equalsIgnoreCase(databaseName) && TBL_NAME_2_TBL_META.containsKey(tableName);
  }

  @Override
  public boolean hasPartition(
      String databaseName, String tableName, List<String> partitionValues) {
    return DB_NAME.equalsIgnoreCase(databaseName)

        && TBL_PARTITIONED.equalsIgnoreCase(tableName)
        && TBL_PARTITIONED_META.getPartitions()
                               .stream()
                               .anyMatch(p -> partitionValues.equals(p.getPartitionValues()));
  }

  @Override
  public List<String> listDatabases() {
    return Collections.singletonList(DB_NAME);
  }

  @Override
  public List<String> listTables(String databaseName) throws Exception {
    if (hasDatabase(databaseName)) {
      return new ArrayList<>(TBL_NAME_2_TBL_META.keySet());
    } else {
      throw new Exception("Database doesn't exist");
    }
  }

  @Override
  public List<String> listResources(String databaseName) throws Exception {
    return null;
  }

  @Override
  public List<String> listFunctions(String databaseName) throws Exception {
    return null;
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) throws Exception {
    if (hasTable(databaseName, tableName)) {
      return TBL_NAME_2_TBL_META
          .get(tableName)
          .getPartitions()
          .stream()
          .map(PartitionMetaModel::getPartitionValues)
          .collect(Collectors.toList());
    } else {
      throw new Exception("Database or table doesn't exist");
    }
  }

  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    if (hasTable(databaseName, tableName)) {
      return TBL_NAME_2_TBL_META.get(tableName);
    } else {
      throw new Exception("Database or table doesn't exist");
    }
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName, String tableName)
      throws Exception {
    if (hasTable(databaseName, tableName)) {
      return TBL_NAME_2_TBL_META.get(tableName);
    } else {
      throw new Exception("Database or table doesn't exist");
    }
  }

  @Override
  public PartitionMetaModel getPartitionMeta(String databaseName, String tableName,
                                             List<String> partitionValues) throws Exception {
    if (hasPartition(databaseName, tableName, partitionValues)) {
      return TBL_NAME_2_TBL_META
          .get(tableName)
          .getPartitions()
          .stream()
          .filter(p -> partitionValues.equals(p.getPartitionValues()))
          .findAny()
          .get();
    } else {
      throw new Exception("Database or table doesn't exist");
    }
  }

  @Override
  public ResourceMetaModel getResourceMeta(String databaseName, String resourceName)
      throws Exception {
    return null;
  }

  @Override
  public FunctionMetaModel getFunctionMeta(String databaseName, String functionName)
      throws Exception {
    return null;
  }

  @Override
  public List<ObjectType> getSupportedObjectTypes() {
    return Collections.singletonList(ObjectType.TABLE);
  }

  @Override
  public void shutdown() {
  }
}
