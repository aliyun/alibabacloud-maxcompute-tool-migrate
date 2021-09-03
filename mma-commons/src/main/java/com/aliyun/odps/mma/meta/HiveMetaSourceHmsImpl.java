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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel.TableMetaModelBuilder;
import com.aliyun.odps.mma.util.GsonUtils;

public class HiveMetaSourceHmsImpl implements MetaSource {

  private static final Logger LOG = LogManager.getLogger(HiveMetaSourceHmsImpl.class);

  private static final String TABLE_PARAMETER_TOTAL_SIZE = "totalSize";
  private static final String TABLE_PARAMETER_CREATE_TIME = "createTime";
  private static final String TABLE_PARAMETER_LAST_DDL_TIME = "transient_lastDdlTime";

  private IMetaStoreClient hmsClient;

  public HiveMetaSourceHmsImpl(
      String hmsUris,
      boolean hmsSaslEnabled,
      String kerberosPrincipal,
      String kerberosKeyTab,
      Map<String, String> javaSecurityConfigs,
      Map<String, String> extraConfigs) throws MetaException {
    initHmsClient(
        hmsUris,
        hmsSaslEnabled,
        kerberosPrincipal,
        kerberosKeyTab,
        javaSecurityConfigs,
        extraConfigs);
  }

  private void initHmsClient(
      String hmsAddr,
      boolean hmsSaslEnabled,
      String kerberosPrincipal,
      String kerberosKeyTab,
      Map<String, String> javaSecurityConfigs,
      Map<String, String> extraConfigs) throws MetaException {
    LOG.info("Initializing HMS client, "
             + "HMS addr: {}, "
             + "kbr principal: {}, "
             + "kbr keytab: {}, "
             + "java security configs: {}, "
             + "extra configs: {}",
             hmsAddr,
             kerberosPrincipal,
             kerberosKeyTab,
             javaSecurityConfigs,
             extraConfigs);

    Configuration conf = new Configuration();

    HiveConf hiveConf = new HiveConf(conf, Configuration.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hmsAddr);

    if (hmsSaslEnabled) {
      LOG.info("Set {} to true", HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
      LOG.info("Set {} to {}", HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, kerberosPrincipal);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, kerberosPrincipal);
      LOG.info("Set {} to {}", HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, kerberosKeyTab);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, kerberosKeyTab);
    }

    for (Entry<String, String> entry : extraConfigs.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    for (Entry<String, String> entry : javaSecurityConfigs.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }

    this.hmsClient = RetryingMetaStoreClient.getProxy(
        hiveConf, tbl -> null, HiveMetaStoreClient.class.getName());
  }

  @Override
  public List<ObjectType> getSupportedObjectTypes() {
    return Collections.singletonList(ObjectType.TABLE);
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

  private TableMetaModel getTableMetaInternal(
      String databaseName,
      String tableName,
      boolean withoutPartitionMeta) throws Exception {
    Table table = hmsClient.getTable(databaseName, tableName);

    List<FieldSchema> columns = hmsClient.getFields(databaseName, tableName);
    List<ColumnMetaModel> columnMetaModels = new ArrayList<>(columns.size());
    for (FieldSchema column : columns) {
      ColumnMetaModel columnMetaModel = new ColumnMetaModel(
          column.getName(),
          column.getType(),
          column.getComment());
      columnMetaModels.add(columnMetaModel);
      LOG.debug("Database: {}, Table: {}, column: {} {}",
                databaseName,
                tableName,
                column.getName(),
                column.getType());
    }

    TableMetaModelBuilder builder = new TableMetaModelBuilder(
        databaseName,
        tableName,
        columnMetaModels);

    List<FieldSchema> partitionColumns = table.getPartitionKeys();
    if (!partitionColumns.isEmpty()) {
      List<ColumnMetaModel> partitionColumnMetaModels = new ArrayList<>(partitionColumns.size());
      for (FieldSchema partitionColumn : partitionColumns) {
        ColumnMetaModel columnMetaModel = new ColumnMetaModel(
            partitionColumn.getName(),
            partitionColumn.getType(),
            partitionColumn.getComment()
        );
        partitionColumnMetaModels.add(columnMetaModel);
        LOG.debug("Database: {}, Table: {}, partition column: {} {}",
                  databaseName,
                  tableName,
                  partitionColumn.getName(),
                  partitionColumn.getType());
      }
      builder.partitionColumns(partitionColumnMetaModels);
    }

    // Table comment is not available
    // LifeCycle not is available

    if (table.isSetParameters()) {
      Map<String, String> parameters = table.getParameters();
      if (parameters.containsKey(TABLE_PARAMETER_TOTAL_SIZE)) {
        try {
          long size = Long.parseLong(parameters.get(TABLE_PARAMETER_TOTAL_SIZE));
          builder.size(size);
          LOG.debug("Database: {}, Table: {}, size: {}", databaseName, tableName, size);
        } catch (NumberFormatException ignore) {
          LOG.warn(
              "Database: {}, Table: {}, illegal size: {}",
              databaseName,
              tableName,
              parameters.get(TABLE_PARAMETER_TOTAL_SIZE));
        }
      }
      if (parameters.containsKey(TABLE_PARAMETER_CREATE_TIME)) {
        try {
          long creationTime = Long.parseLong(parameters.get(TABLE_PARAMETER_CREATE_TIME));
          builder.creationTime(creationTime);
          LOG.debug("Database: {}, Table: {}, ctime: {}", databaseName, tableName, creationTime);
        } catch (NumberFormatException ignore) {
          LOG.warn(
              "Database: {}, Table: {}, illegal ctime: {}",
              databaseName,
              tableName,
              parameters.get(TABLE_PARAMETER_CREATE_TIME));
        }
      }
      if (parameters.containsKey(TABLE_PARAMETER_LAST_DDL_TIME)) {
        try {
          long lastDdlTime = Long.parseLong(parameters.get(TABLE_PARAMETER_LAST_DDL_TIME));
          builder.lastModificationTime(lastDdlTime);
          LOG.debug("Database: {}, Table: {}, mtime: {}", databaseName, tableName, lastDdlTime);
        } catch (NumberFormatException ignore) {
          LOG.warn(
              "Database: {}, Table: {}, illegal mtime: {}",
              databaseName,
              tableName,
              parameters.get(TABLE_PARAMETER_LAST_DDL_TIME));
        }
      }
    }

    String location = table.getSd().getLocation();
    if (!StringUtils.isBlank(location)) {
      builder.location(location);
      LOG.debug(
          "Database: {}, Table: {}, location: {}", databaseName, tableName, location);
    }

    String inputFormat = table.getSd().getInputFormat();
    if (!StringUtils.isBlank(inputFormat)) {
      builder.inputFormat(inputFormat);
      LOG.debug(
          "Database: {}, Table: {}, input format: {}", databaseName, tableName, inputFormat);
    }

    String outputFormat = table.getSd().getOutputFormat();
    if (!StringUtils.isBlank(outputFormat)) {
      builder.outputFormat(outputFormat);
      LOG.debug(
          "Database: {}, Table: {}, output format: {}", databaseName, tableName, outputFormat);
    }

    String serDe = table.getSd().getSerdeInfo().getSerializationLib();
    if (StringUtils.isBlank(serDe)) {
      builder.serDe(serDe);
      LOG.debug("Database: {}, Table: {}, serde lib: {}", databaseName, tableName, serDe);
    }

    if (table.getSd().getSerdeInfo().isSetParameters()) {
      builder.serDeProperties(table.getSd().getSerdeInfo().getParameters());
      table.getSd().getSerdeInfo().getParameters().forEach((key, value) -> LOG.debug(
          "Database: {}, Table: {}, serde property key: {}, value: {}",
          databaseName,
          tableName,
          key,
          value));
    }

    // Get partition meta for partitioned tables
    if (!withoutPartitionMeta && partitionColumns.size() > 0) {
      List<Partition> partitions = hmsClient.listPartitions(databaseName, tableName, (short) -1);
      LOG.info(
          "Database: {}, Table: {}, number of partitions: {}",
          databaseName,
          tableName,
          partitions.size());
      List<PartitionMetaModel> partitionMetaModels = new ArrayList<>(partitions.size());
      for (Partition partition : partitions) {
        partitionMetaModels.add(
            getPartitionMeta(databaseName, tableName, partition.getValues()));
        LOG.debug("Database: {}, Table: {}, partition: {} ",
                  databaseName,
                  tableName,
                  GsonUtils.GSON.toJson(partition));
      }
      builder.partitions(partitionMetaModels);
    }

    return builder.build();
  }

  @Override
  public PartitionMetaModel getPartitionMeta(
      String databaseName,
      String tableName,
      List<String> partitionValues) throws Exception {
    Partition partition = hmsClient.getPartition(databaseName, tableName, partitionValues);

    Long mtime = null;
    Long size = null;
    if (partition.isSetParameters()) {
      Map<String, String> parameters = partition.getParameters();
      if (parameters.containsKey(TABLE_PARAMETER_LAST_DDL_TIME)) {
        try {
          mtime = Long.parseLong(parameters.get(TABLE_PARAMETER_LAST_DDL_TIME));
          LOG.debug("Database: {}, Table: {}, Partition: {}, mtime: {}",
                    databaseName,
                    tableName,
                    partitionValues,
                    mtime);
        } catch (NumberFormatException ignore) {
          LOG.warn(
              "Database: {}, Table: {}, Partition: {}, illegal mtime: {}",
              databaseName,
              tableName,
              partitionValues,
              parameters.get(TABLE_PARAMETER_LAST_DDL_TIME));
        }
      }
      if (parameters.containsKey(TABLE_PARAMETER_TOTAL_SIZE)) {
        try {
          size = Long.parseLong(parameters.get(TABLE_PARAMETER_TOTAL_SIZE));
          LOG.debug("Database: {}, Table: {}, Partition: {}, size: {}",
                    databaseName,
                    tableName,
                    partitionValues,
                    size);
        } catch (NumberFormatException ignore) {
          LOG.warn(
              "Database: {}, Table: {}, Partition: {}, illegal size: {}",
              databaseName,
              tableName,
              partitionValues,
              parameters.get(TABLE_PARAMETER_TOTAL_SIZE));
        }
      }
    }

    return new PartitionMetaModel(
        partitionValues,
        partition.getSd().getLocation(),
        (long) partition.getCreateTime(),
        mtime,
        size);
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
