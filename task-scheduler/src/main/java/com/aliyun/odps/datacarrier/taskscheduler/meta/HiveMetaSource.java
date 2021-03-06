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

package com.aliyun.odps.datacarrier.taskscheduler.meta;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.utils.StringUtils;

public class HiveMetaSource implements MetaSource {

  private static final Logger LOG = LogManager.getLogger(HiveMetaSource.class);

  private IMetaStoreClient hmsClient;
  private FileSystem fs;

  public HiveMetaSource(String hmsAddr,
                        Map<String, String> hdfsConfigs,
                        String principal,
                        String keyTab,
                        List<String> systemProperties) throws MetaException {
    initHmsClient(hmsAddr, principal, keyTab, systemProperties);
    initFileSystem(hdfsConfigs, principal, keyTab);
  }

  private void initHmsClient(String hmsAddr,
                             String principal,
                             String keyTab,
                             List<String> systemProperties) throws MetaException {
    LOG.info("Initializing HMS client, "
             + "HMS addr: {}, "
             + "kbr principal: {}, "
             + "kbr keytab: {}, "
             + "system properties: {}",
             hmsAddr,
             principal,
             keyTab,
             systemProperties != null ? String.join(" ", systemProperties) : "null");

    Configuration conf = new Configuration();
    //TODO: support user defined hadoop configurations, e.g. HADOOP_RPC_PROTECTION=privacy
    HiveConf hiveConf = new HiveConf(conf, Configuration.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hmsAddr);
    if (!StringUtils.isNullOrEmpty(principal)) {
      LOG.info("Set {} to true", HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
      LOG.info("Set {} to {}", HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
    }
    if (!StringUtils.isNullOrEmpty(keyTab)) {
      LOG.info("Set {} to {}", HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keyTab);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keyTab);
    }
    if (systemProperties != null && systemProperties.size() > 0) {
      for (String property : systemProperties) {
        int idx = property.indexOf('=');
        if (idx != -1) {
          LOG.info("Set system property {} = {}",
                   property.substring(0, idx),
                   property.substring(idx + 1));
          System.setProperty(property.substring(0, idx), property.substring(idx + 1));
        } else {
          LOG.error("Invalid system property: " + property);
        }
      }
    }

    this.hmsClient = RetryingMetaStoreClient.getProxy(
        hiveConf, tbl -> null, HiveMetaStoreClient.class.getName());
  }

  private void initFileSystem(
      Map<String, String> hdfsConfigs,
      String principal,
      String keyTab) {

    LOG.info("Initializing HDFS client with: {}", hdfsConfigs);
    if (hdfsConfigs == null || hdfsConfigs.isEmpty()) {
      return;
    }

    Configuration conf = new Configuration();
    for (Entry<String, String> entry : hdfsConfigs.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    UserGroupInformation.setConfiguration(conf);
    try {
      UserGroupInformation.loginUserFromKeytab(principal, keyTab);
      fs = FileSystem.get(conf);
    } catch (Exception e) {
      LOG.warn("Initializing HDFS client failed", e);
    }
  }

  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    return getTableMetaInternal(databaseName, tableName, false);
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                         String tableName) throws Exception {
    return getTableMetaInternal(databaseName, tableName, true);
  }

  private TableMetaModel getTableMetaInternal(String databaseName,
                                              String tableName,
                                              boolean withoutPartitionMeta) throws Exception {
    Table table = hmsClient.getTable(databaseName, tableName);

    TableMetaModel tableMetaModel = new TableMetaModel();
    tableMetaModel.databaseName = databaseName;
    tableMetaModel.tableName = tableName;
    tableMetaModel.location = table.getSd().getLocation();
    LOG.debug("Database: {}, Table: {}, location: {}",
              databaseName, tableName, table.getSd().getLocation());
    tableMetaModel.inputFormat = table.getSd().getInputFormat();
    LOG.debug("Database: {}, Table: {}, input format: {}",
              databaseName, tableName, table.getSd().getInputFormat());
    tableMetaModel.outputFormat = table.getSd().getOutputFormat();
    LOG.debug("Database: {}, Table: {}, output format: {}",
              databaseName, tableName, table.getSd().getOutputFormat());
    tableMetaModel.serDe = table.getSd().getSerdeInfo().getSerializationLib();
    LOG.debug("Database: {}, Table: {}, serde lib: {}",
              databaseName, tableName, table.getSd().getSerdeInfo().getSerializationLib());
    if (table.getSd().getSerdeInfo().isSetParameters()) {
      tableMetaModel.serDeProperties.putAll(table.getSd().getSerdeInfo().getParameters());
      table.getSd().getSerdeInfo().getParameters().forEach((key, value) -> {
        LOG.debug("Database: {}, Table: {}, serde property key: {}, value: {}",
                  databaseName,
                  tableName,
                  key,
                  value);
      });
    }
    if (table.isSetParameters()) {
      Map<String, String> parameters = table.getParameters();
      if (parameters.containsKey("transient_lastDdlTime")) {
        try {
          tableMetaModel.lastModifiedTime =
              Long.parseLong(parameters.get("transient_lastDdlTime"));
          LOG.debug("Database: {}, Table: {}, mtime: {}",
                    databaseName,
                    tableName,
                    Long.parseLong(parameters.get("transient_lastDdlTime")));
        } catch (NumberFormatException ignore) {
        }
      }
    }

    if (fs != null) {
      Path path = new Path(tableMetaModel.location);
      tableMetaModel.size = fs.getContentSummary(path).getLength();
      LOG.debug("Database: {}, Table: {}, size: {}",
                databaseName,
                tableName,
                tableMetaModel.size);
    }

    List<FieldSchema> columns = hmsClient.getFields(databaseName, tableName);
    for (FieldSchema column : columns) {
      ColumnMetaModel columnMetaModel = new ColumnMetaModel();
      columnMetaModel.columnName = column.getName();
      columnMetaModel.type = column.getType();
      columnMetaModel.comment = column.getComment();
      tableMetaModel.columns.add(columnMetaModel);
      LOG.debug("Database: {}, Table: {}, column: {} {}",
                databaseName,
                tableName,
                column.getName(),
                column.getType());
    }

    List<FieldSchema> partitionColumns = table.getPartitionKeys();
    for (FieldSchema partitionColumn : partitionColumns) {
      ColumnMetaModel columnMetaModel = new ColumnMetaModel();
      columnMetaModel.columnName = partitionColumn.getName();
      columnMetaModel.type = partitionColumn.getType();
      columnMetaModel.comment = partitionColumn.getComment();
      tableMetaModel.partitionColumns.add(columnMetaModel);
      LOG.debug("Database: {}, Table: {}, partition column: {} {}",
                databaseName,
                tableName,
                partitionColumn.getName(),
                partitionColumn.getType());
    }

    // Get partition meta for partitioned tables
    if (!withoutPartitionMeta && partitionColumns.size() > 0) {
      List<Partition> partitions = hmsClient.listPartitions(databaseName, tableName, (short) -1);
      LOG.info("Database: {}, Table: {}, number of partitions: {}",
                databaseName, tableName, partitions.size());
      for (Partition partition : partitions) {
        PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
        partitionMetaModel.createTime = (long) partition.getCreateTime();
        partitionMetaModel.location = partition.getSd().getLocation();
        partitionMetaModel.partitionValues = partition.getValues();
        setPartitionSize(partitionMetaModel);

        tableMetaModel.partitions.add(partitionMetaModel);
        LOG.debug("Database: {}, Table: {}, partition: {} ",
                  databaseName,
                  tableName,
                  partition.getValues());
      }
    }

    return tableMetaModel;
  }

  @Override
  public PartitionMetaModel getPartitionMeta(String databaseName, String tableName,
                                             List<String> partitionValues) throws Exception {
    PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
    Partition partition = hmsClient.getPartition(databaseName, tableName, partitionValues);
    partitionMetaModel.createTime = (long) partition.getCreateTime();
    if (partition.isSetParameters()) {
      Map<String, String> parameters = partition.getParameters();
      if (parameters.containsKey("transient_lastDdlTime")) {
        try {
          partitionMetaModel.lastModifiedTime =
              Long.parseLong(parameters.get("transient_lastDdlTime"));
          LOG.debug("Database: {}, Table: {}, Partition: {}, mtime: {}",
                    databaseName,
                    tableName,
                    partitionValues,
                    Long.parseLong(parameters.get("transient_lastDdlTime")));
        } catch (NumberFormatException ignore) {
        }
      }
    }
    partitionMetaModel.location = partition.getSd().getLocation();
    LOG.debug("Database: {}, Table: {}, Partition: {}, location: {}",
              databaseName,
              tableName,
              partitionValues,
              partition.getSd().getLocation());
    partitionMetaModel.partitionValues = partition.getValues();
    setPartitionSize(partitionMetaModel);

    return partitionMetaModel;
  }



  public void setPartitionSize(PartitionMetaModel partitionMetaModel) throws IOException {
    if (fs != null) {
      Path location = new Path(partitionMetaModel.location);
      partitionMetaModel.size = fs.getContentSummary(location).getLength();
      LOG.debug("Location: {}, size: {}", location, partitionMetaModel.size);
    }
  }

  @Override
  public boolean hasDatabase(String databaseName) throws Exception {
    try {
      hmsClient.getDatabase(databaseName);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean hasTable(String databaseName, String tableName) throws Exception {
    try {
      hmsClient.getTable(databaseName, tableName);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception {
    try {
      hmsClient.getPartition(databaseName, tableName, partitionValues);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  @Override
  public List<String> listDatabases() throws Exception {
    List<String> databases = hmsClient.getAllDatabases();
    LOG.debug("Databases: {}", databases);

    return databases;
  }


  @Override
  public List<String> listTables(String databaseName) throws Exception {
    return hmsClient.getAllTables(databaseName);
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) throws Exception {
    List<List<String>> partitionValuesList = new LinkedList<>();
    List<Partition> partitions = hmsClient.listPartitions(databaseName, tableName, (short) -1);
    LOG.info("Database: {}, Table: {}, number of partitions: {}",
              databaseName, tableName, partitions.size());
    for (Partition partition : partitions) {
      partitionValuesList.add(partition.getValues());
      LOG.debug("Database: {}, Table: {}, partition: {} ",
                databaseName,
                tableName,
                partition.getValues());
    }
    return partitionValuesList;
  }

  @Override
  public void shutdown() {
    hmsClient.close();
  }
}
