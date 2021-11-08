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
 *
 */

package com.aliyun.odps.mma.meta.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;

public class TableMetaModel {

  public static class TableMetaModelBuilder {

    TableMetaModel tableMetaModel;

    public TableMetaModelBuilder(TableMetaModel tableMetaModel) {
      this.tableMetaModel = new TableMetaModel();
      this.tableMetaModel.database = Validate.notNull(tableMetaModel.database);
      this.tableMetaModel.table = Validate.notNull(tableMetaModel.table);
      this.tableMetaModel.columns = new ArrayList<>(Validate.notNull(tableMetaModel.columns));
      this.tableMetaModel.columns.forEach(Validate::notNull);
      this.tableMetaModel.tableStorage = tableMetaModel.tableStorage;
      this.tableMetaModel.comment = tableMetaModel.comment;
      this.tableMetaModel.lifeCycle = tableMetaModel.lifeCycle;
      this.tableMetaModel.size = tableMetaModel.size;
      this.tableMetaModel.location = tableMetaModel.location;
      this.tableMetaModel.inputFormat = tableMetaModel.inputFormat;
      this.tableMetaModel.outputFormat = tableMetaModel.outputFormat;
      this.tableMetaModel.serDe = tableMetaModel.serDe;
      this.tableMetaModel.creationTime = tableMetaModel.creationTime;
      this.tableMetaModel.lastModificationTime = tableMetaModel.lastModificationTime;
      this.tableMetaModel.serDeProperties = new HashMap<>(tableMetaModel.serDeProperties);
      this.tableMetaModel.partitionColumns = new ArrayList<>(tableMetaModel.partitionColumns);
      this.tableMetaModel.partitions = new ArrayList<>(tableMetaModel.partitions);
    }

    public TableMetaModelBuilder(
        String database,
        String table,
        List<ColumnMetaModel> columnMetaModels) {
      this.tableMetaModel = new TableMetaModel();
      this.tableMetaModel.database = Validate.notNull(database);
      this.tableMetaModel.table = Validate.notNull(table);
      this.tableMetaModel.columns = new ArrayList<>(Validate.notNull(columnMetaModels));
      this.tableMetaModel.columns.forEach(Validate::notNull);
    }

    public TableMetaModelBuilder database(String database) {
      this.tableMetaModel.database = Validate.notNull(database);
      return this;
    }

    public TableMetaModelBuilder table(String table) {
      this.tableMetaModel.table = Validate.notNull(table);
      return this;
    }

    public TableMetaModelBuilder tableStorage(String storage) {
      this.tableMetaModel.tableStorage = storage;
      return this;
    }

    public TableMetaModelBuilder comment(String comment) {
      this.tableMetaModel.comment = comment;
      return this;
    }

    public TableMetaModelBuilder lifeCycle(Long lifeCycle) {
      this.tableMetaModel.lifeCycle = lifeCycle;
      return this;
    }

    public TableMetaModelBuilder size(Long size) {
      this.tableMetaModel.size = size;
      return this;
    }

    public TableMetaModelBuilder location(String location) {
      this.tableMetaModel.location = location;
      return this;
    }

    public TableMetaModelBuilder inputFormat(String inputFormat) {
      this.tableMetaModel.inputFormat = inputFormat;
      return this;
    }

    public TableMetaModelBuilder outputFormat(String outputFormat) {
      this.tableMetaModel.outputFormat = outputFormat;
      return this;
    }

    public TableMetaModelBuilder serDe(String serDe) {
      this.tableMetaModel.serDe = serDe;
      return this;
    }

    public TableMetaModelBuilder creationTime(Long creationTime) {
      this.tableMetaModel.creationTime = creationTime;
      return this;
    }

    public TableMetaModelBuilder lastModificationTime(Long lastModificationTime) {
      this.tableMetaModel.lastModificationTime = lastModificationTime;
      return this;
    }

    public TableMetaModelBuilder serDeProperties(Map<String, String> serDeProperties) {
      if (serDeProperties != null) {
        this.tableMetaModel.serDeProperties = new HashMap<>(serDeProperties);
      } else {
        this.tableMetaModel.serDeProperties = new HashMap<>();
      }
      return this;
    }

    public TableMetaModelBuilder partitionColumns(
        List<ColumnMetaModel> partitionColumns) {
      if (partitionColumns != null) {
        this.tableMetaModel.partitionColumns = new ArrayList<>(Validate.notNull(partitionColumns));
      } else {
        this.tableMetaModel.partitionColumns = new ArrayList<>(0);
      }
      this.tableMetaModel.partitionColumns.forEach(Validate::notNull);
      return this;
    }

    public TableMetaModelBuilder partitions(List<PartitionMetaModel> partitions) {
      if (partitions != null) {
        this.tableMetaModel.partitions = new ArrayList<>(partitions);
      } else {
        this.tableMetaModel.partitions = new ArrayList<>(0);
      }
      this.tableMetaModel.partitions.forEach(Validate::notNull);
      return this;
    }

    public TableMetaModel build() {
      return tableMetaModel;
    }
  }

  private String database;
  private String table;
  // Specify the table storage, such as OSS
  private String tableStorage;
  private String comment;
  private Long lifeCycle;
  // Size in bytes
  private Long size;
  private String location;
  private String inputFormat;
  private String outputFormat;
  private String serDe;
  private Long creationTime;
  // In seconds
  private Long lastModificationTime;
  private Map<String, String> serDeProperties = Collections.emptyMap();
  private List<ColumnMetaModel> columns;
  private List<ColumnMetaModel> partitionColumns = Collections.emptyList();
  private List<PartitionMetaModel> partitions = Collections.emptyList();

  private TableMetaModel() {
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public String getTableStorage() {
    return tableStorage;
  }

  public String getComment() {
    return comment;
  }

  public Long getLifeCycle() {
    return lifeCycle;
  }

  public Long getSize() {
    return size;
  }

  public String getLocation() {
    return location;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public String getSerDe() {
    return serDe;
  }

  public Long getCreationTime() {
    return creationTime;
  }

  public Long getLastModificationTime() {
    return lastModificationTime;
  }

  public Map<String, String> getSerDeProperties() {
    return new HashMap<>(serDeProperties);
  }

  public List<ColumnMetaModel> getColumns() {
    return new ArrayList<>(columns);
  }

  public List<ColumnMetaModel> getPartitionColumns() {
    return new ArrayList<>(partitionColumns);
  }

  public List<PartitionMetaModel> getPartitions() {
    return new ArrayList<>(partitions);
  }

  @Override
  public TableMetaModel clone() {
    TableMetaModel tableMetaModel = new TableMetaModel();
    tableMetaModel.tableStorage = this.tableStorage;
    tableMetaModel.comment = this.comment;
    tableMetaModel.lifeCycle = this.lifeCycle;
    tableMetaModel.size = this.size;
    tableMetaModel.location = this.location;
    tableMetaModel.inputFormat = this.inputFormat;
    tableMetaModel.outputFormat = this.outputFormat;
    tableMetaModel.serDe = this.serDe;
    tableMetaModel.creationTime = this.creationTime;
    tableMetaModel.lastModificationTime = this.lastModificationTime;
    tableMetaModel.serDeProperties = new HashMap<>(this.serDeProperties);
    tableMetaModel.columns = new ArrayList<>(this.columns);
    tableMetaModel.partitionColumns = new ArrayList<>(this.partitionColumns);
    tableMetaModel.partitions = new ArrayList<>(partitions);
    return tableMetaModel;
  }
}
