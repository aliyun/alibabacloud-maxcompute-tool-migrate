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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.odps.ArchiveResource;
import com.aliyun.odps.Column;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.PyResource;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.meta.model.ColumnMetaModel;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel.TableMetaModelBuilder;
import com.aliyun.odps.utils.StringUtils;

public class McMetaSource implements MetaSource {

  private static final List<ObjectType> SUPPORTED_OBJECT_TYPES;
  static {
    SUPPORTED_OBJECT_TYPES = new ArrayList<>(1);
    SUPPORTED_OBJECT_TYPES.add(ObjectType.TABLE);
    SUPPORTED_OBJECT_TYPES.add(ObjectType.RESOURCE);
    SUPPORTED_OBJECT_TYPES.add(ObjectType.FUNCTION);
  }

  private Odps odps;

  public McMetaSource(
      String accessId,
      String accessKey,
      String endpoint) {
    Account account = new AliyunAccount(accessId, accessKey);
    odps = new Odps(account);
    odps.setUserAgent("MMA");
    odps.setEndpoint(endpoint);
  }

  @Override
  public List<ObjectType> getSupportedObjectTypes() {
    return new ArrayList<>(SUPPORTED_OBJECT_TYPES);
  }

  @Override
  public boolean hasDatabase(String databaseName) throws Exception {
    return odps.projects().exists(databaseName);
  }

  @Override
  public List<String> listDatabases() {
    List<String> databases = new ArrayList<>();
    Iterator<Project> iterator = odps.projects().iterator(null);
    while (iterator.hasNext()) {
      databases.add(iterator.next().getName());
    }
    return databases;
  }

  @Override
  public boolean hasTable(String databaseName, String tableName) throws Exception {
    return odps.tables().exists(databaseName, tableName);
  }

  @Override
  public List<String> listTables(String databaseName) {
    List<String> tables = new ArrayList<>();
    Iterator<Table> iterator = odps.tables().iterator(databaseName);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      if (table.isVirtualView()) {
        continue;
      }
      tables.add(table.getName());
    }
    return tables;
  }

  @Override
  public List<String> listResources(String databaseName) throws Exception {
    List<String> resources = new ArrayList<>();
    Iterator<Resource> iterator = odps.resources().iterator(databaseName);
    while(iterator.hasNext()) {
      Resource resource = iterator.next();
      resources.add(resource.getName());
    }
    return resources;
  }

  @Override
  public List<String> listFunctions(String databaseName) throws Exception {
    List<String> functions = new ArrayList<>();
    Iterator<Function> iterator = odps.functions().iterator(databaseName);
    while(iterator.hasNext()) {
      Function function = iterator.next();
      functions.add(function.getName());
    }
    return functions;
  }

//  public List<String> listViews(String databaseName) {
//    List<String> views = new ArrayList<>();
//    Iterator<Table> iterator = odps.tables().iterator(databaseName);
//    while (iterator.hasNext()) {
//      Table table = iterator.next();
//      if (table.isVirtualView()) {
//        views.add(table.getName());
//      }
//    }
//    return views;
//  }
//
//  public List<String> listManagedTables(String databaseName) {
//    List<String> tables = new ArrayList<>();
//    Iterator<Table> iterator = odps.tables().iterator(databaseName);
//    while (iterator.hasNext()) {
//      Table table = iterator.next();
//      if (table.isExternalTable() || table.isVirtualView()) {
//        continue;
//      }
//      tables.add(table.getName());
//    }
//    return tables;
//  }

  @Override
  public TableMetaModel getTableMeta(
      String databaseName, String tableName) {
    return getTableMetaInternal(databaseName, tableName, true);
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(
      String databaseName, String tableName) {
    return getTableMetaInternal(databaseName, tableName, false);
  }

  @Override
  public boolean hasPartition(
      String databaseName, String tableName, List<String> partitionValues) throws OdpsException {
    if (!odps.tables().exists(databaseName, tableName)) {
      return false;
    }

    List<Column> partitionColumns =
        odps.tables().get(databaseName, tableName).getSchema().getPartitionColumns();
    if (partitionValues.size() != partitionColumns.size()) {
      return false;
    }

    PartitionSpec partitionSpec = new PartitionSpec();
    for (int i = 0; i < partitionValues.size(); i++) {
      partitionSpec.set(partitionColumns.get(i).getName(), partitionValues.get(i));
    }

    return odps.tables().get(databaseName, tableName).hasPartition(partitionSpec);
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) {
    List<List<String>> partitionValuesList = new LinkedList<>();
    Iterator<Partition> partitionIter =
        odps.tables().get(databaseName, tableName).getPartitionIterator();
    while (partitionIter.hasNext()) {
      Partition partition = partitionIter.next();
      PartitionSpec partitionSpec = partition.getPartitionSpec();
      List<String> partitionValues = new ArrayList<>();
      for (String key : partitionSpec.keys()) {
        partitionValues.add(partitionSpec.get(key));
      }
      partitionValuesList.add(partitionValues);
    }
    return partitionValuesList;
  }

  @Override
  public PartitionMetaModel getPartitionMeta(
      String databaseName, String tableName, List<String> partitionValues) {
    Table table = odps.tables().get(databaseName, tableName);
    List<Column> partitionColumns = table.getSchema().getPartitionColumns();
    PartitionSpec partitionSpec = new PartitionSpec();
    for (int partIndex = 0; partIndex < partitionValues.size(); partIndex++) {
      partitionSpec.set(partitionColumns.get(partIndex).getName(), partitionValues.get(partIndex));
    }
    Partition partition = table.getPartition(partitionSpec);
    return getPartitionMetaModelInternal(partition);
  }

  @Override
  public ResourceMetaModel getResourceMeta(String databaseName, String resourceName)
      throws Exception {
    if (!odps.resources().exists(databaseName, resourceName)) {
      throw new MmaException("resource " + databaseName + "." + resourceName + " does not exist");
    }
    Resource resource = odps.resources().get(databaseName, resourceName);
    String tableName = null;
    String partitionSpec = null;
    if (Resource.Type.TABLE.equals(resource.getType())) {
      TableResource tableResource = (TableResource) resource;
      tableName = tableResource.getSourceTable().getName();
      PartitionSpec spec = tableResource.getSourceTablePartition();
      if (spec != null) {
        partitionSpec = spec.toString();
      }
    }
    return new ResourceMetaModel(resource.getName(),
                                 resource.getType(),
                                 resource.getComment(),
                                 tableName,
                                 partitionSpec);
  }

  @Override
  public FunctionMetaModel getFunctionMeta(String databaseName, String functionName)
      throws Exception {
    if (!odps.functions().exists(databaseName, functionName)) {
      throw new MmaException("function " + databaseName + "." + functionName + " does not exist");
    }
    Function function = odps.functions().get(databaseName, functionName);
    List<String> resources = new ArrayList<>();
    function.getResourceNames();
    for (Resource resource : function.getResources()) {
      resources.add(resource.getName());
    }
    return new FunctionMetaModel(function.getName(), function.getClassPath(), resources);
  }

  @Override
  public void shutdown() {
    this.odps = null;
  }

  private TableMetaModel getTableMetaInternal(
      String databaseName, String tableName, boolean withPartition) {
    Table table = odps.tables().get(databaseName, tableName);
    return getTableMetaInternal(table, withPartition);
  }

  private TableMetaModel getTableMetaInternal(Table table, boolean withPartition) {
    TableSchema tableSchema = table.getSchema();
    List<ColumnMetaModel> columnMetaModels = tableSchema
        .getColumns()
        .stream()
        .map(this::getColumnMetaModelInternal)
        .collect(Collectors.toList());

    TableMetaModelBuilder builder = new TableMetaModelBuilder(
        table.getProject(),
        table.getName(),
        columnMetaModels);

    builder.comment(table.getComment());
    builder.lifeCycle(table.getLife());
    builder.size(table.getSize());
    builder.location(table.getLocation());

    // Input format is not available
    // Output format is not available
    // SerDe is not available

    builder.creationTime(table.getCreatedTime().getTime() / 1000L);
    builder.lastModificationTime(table.getLastDataModifiedTime().getTime() / 1000L);

    if (table.getSerDeProperties() != null) {
      builder.serDeProperties(table.getSerDeProperties());
    }

    if (!tableSchema.getPartitionColumns().isEmpty()) {
      List<ColumnMetaModel> partitionColumnMetaModels =
          new ArrayList<>(tableSchema.getPartitionColumns().size());
      for (Column column : tableSchema.getPartitionColumns()) {
        partitionColumnMetaModels.add(getColumnMetaModelInternal(column));
      }
      builder.partitionColumns(partitionColumnMetaModels);
    }

    if (withPartition && !tableSchema.getPartitionColumns().isEmpty()) {
      List<Partition> partitions = table.getPartitions();
      List<PartitionMetaModel> partitionMetaModels = new ArrayList<>(partitions.size());
      for (Partition partition : table.getPartitions()) {
        partitionMetaModels.add(getPartitionMetaModelInternal(partition));
      }
      builder.partitions(partitionMetaModels);
    }
    return builder.build();
  }

  private ColumnMetaModel getColumnMetaModelInternal(Column column) {
    return new ColumnMetaModel(
        column.getName(),
        column.getTypeInfo().getTypeName(),
        column.getComment());
  }

  private PartitionMetaModel getPartitionMetaModelInternal(Partition partition) {
    PartitionSpec partitionSpec = partition.getPartitionSpec();
    List<String> partitionValues = new ArrayList<>(partitionSpec.keys().size());
    for (String key : partitionSpec.keys()) {
      partitionValues.add(partitionSpec.get(key));
    }

    return new PartitionMetaModel(
        partitionValues,
        null,
        partition.getCreatedTime().getTime() / 1000L,
        partition.getLastDataModifiedTime().getTime() / 1000L,
        partition.getSize());
  }
}
