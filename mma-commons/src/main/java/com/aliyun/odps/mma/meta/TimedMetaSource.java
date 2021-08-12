package com.aliyun.odps.mma.meta;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class TimedMetaSource implements MetaSource {

  private static final Logger LOG = LogManager.getLogger(TimedMetaSource.class);

  private static long enter(String methodName) {
    long startTime = System.currentTimeMillis();
    LOG.info("Enter {}, start time: {}", methodName, startTime);
    return startTime;
  }

  private static void leave(String methodName, long startTime) {
    LOG.info("Leave {}, elapsed time: {} ms", methodName, System.currentTimeMillis() - startTime);
  }

  @Override
  public final boolean hasDatabase(String databaseName) throws Exception {
    long startTime = enter("hasDatabase");
    try {
      return timedHasDatabase(databaseName);
    } finally {
      leave("hasDatabase", startTime);
    }
  }

  abstract boolean timedHasDatabase(String databaseName) throws Exception;

  @Override
  public final boolean hasTable(String databaseName, String tableName) throws Exception {
    long startTime = enter("hasTable");
    try {
      return timedHasTable(databaseName, tableName);
    } finally {
      leave("hasTable", startTime);
    }
  }

  abstract boolean timedHasTable(String databaseName, String tableName) throws Exception;

  @Override
  public final boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception {
    long startTime = enter("hasPartition");
    try {
      return timedHasPartition(databaseName, tableName, partitionValues);
    } finally {
      leave("hasPartition", startTime);
    }
  }

  abstract boolean timedHasPartition(
      String databaseName,
      String tableName,
      List<String> partitionValues) throws Exception;

  @Override
  public final List<String> listDatabases() throws Exception {
    long startTime = enter("listDatabases");
    try {
      return timedListDatabases();
    } finally {
      leave("listDatabases", startTime);
    }
  }

  abstract List<String> timedListDatabases() throws Exception;

  @Override
  public final List<String> listTables(String databaseName) throws Exception {
    long startTime = enter("listTables");
    try {
      return timedListTables(databaseName);
    } finally {
      leave("listTables", startTime);
    }
  }

  abstract List<String> timedListTables(String databaseName) throws Exception;

  @Override
  public final List<List<String>> listPartitions(String databaseName, String tableName) throws Exception {
    long startTime = enter("listPartitions");
    try {
      return timedListPartitions(databaseName, tableName);
    } finally {
      leave("listPartitions", startTime);
    }
  }

  abstract List<List<String>> timedListPartitions(
      String databaseName,
      String tableName) throws Exception;

  @Override
  public final TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    long startTime = enter("getTableMeta");
    try {
      return timedGetTableMeta(databaseName, tableName);
    } finally {
      leave("getTableMeta", startTime);
    }
  }

  abstract TableMetaModel timedGetTableMeta(String databaseName, String tableName) throws Exception;

  @Override
  public final TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName, String tableName)
      throws Exception {
    long startTime = enter("getTableMetaWithoutPartitionMeta");
    try {
      return timedGetTableMetaWithoutPartitionMeta(databaseName, tableName);
    } finally {
      leave("getTableMetaWithoutPartitionMeta", startTime);
    }
  }

  abstract TableMetaModel timedGetTableMetaWithoutPartitionMeta(
      String databaseName,
      String tableName) throws Exception;

  @Override
  public final PartitionMetaModel getPartitionMeta(
      String databaseName,
      String tableName,
      List<String> partitionValues) throws Exception {
    long startTime = enter("getPartitionMeta");
    try {
      return timedGetPartitionMeta(databaseName, tableName, partitionValues);
    } finally {
      leave("getPartitionMeta", startTime);
    }
  }

  abstract PartitionMetaModel timedGetPartitionMeta(
      String databaseName,
      String tableName,
      List<String> partitionValues) throws Exception;
}
