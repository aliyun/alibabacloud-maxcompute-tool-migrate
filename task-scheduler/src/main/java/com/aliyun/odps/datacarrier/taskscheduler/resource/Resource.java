package com.aliyun.odps.datacarrier.taskscheduler.resource;

public enum Resource {
  /**
   * Represents a data transfer job with Hive SQL. Each
   * {@link com.aliyun.odps.datacarrier.taskscheduler.action.HiveUdtfDataTransferAction}
   * requires one 'HIVE_DATA_TRANSFER_JOB_RESOURCE'.
   */
  HIVE_DATA_TRANSFER_JOB_RESOURCE,
  // mapreduce.job.running.map.limit
  /**
   * Represents a worker of a data transfer job with Hive SQL. By default, each
   * {@link com.aliyun.odps.datacarrier.taskscheduler.action.HiveSqlAction}
   * requires 5 'HIVE_DATA_TRANSFER_WORKER_RESOURCE'.
   */
  HIVE_DATA_TRANSFER_WORKER_RESOURCE,
  /**
   * Represents an operation on the metadata of a MaxCompute object (table, resource, e.g.).
   */
  MC_METADATA_OPERATION_RESOURCE
}
