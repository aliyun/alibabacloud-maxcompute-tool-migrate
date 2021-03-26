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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Constants {

  public static final String HELP = "help";
  public static final String PROJECT_NAME = "project_name";
  public static final String ACCESS_ID = "access_id";
  public static final String ACCESS_KEY = "access_key";
  public static final String END_POINT = "end_point";
  public static final String TUNNEL_ENDPOINT = "tunnel_endpoint";

  /*
    Performance related constants
   */
  public static final int MAX_PARTITION_GROUP_SIZE = 200;
  public static final int DEFAULT_PARTITION_GROUP_SPLIT_SIZE_IN_GB = 10;
  public static final int DEFAULT_MAPREDUCE_SPLIT_SIZE_IN_BYTE = 512000000;

  public static final String MMA_TEMPORARY_TABLE_PREFIX = "_temporary_table_generated_by_mma_";

  /*
    Constants of MmaMetaManagerDbImpl
   */
  public static final String DB_FILE_NAME = ".MmaMeta";

  // Types
  public static final String VARCHAR_16 = "VARCHAR(16)";
  public static final String VARCHAR_255 = "VARCHAR(255)";
  public static final String VARCHAR_65535 = "VARCHAR(65535)";
  public static final String TEXT = "TEXT";
  public static final String INT = "INT";
  public static final String BIGINT = "BIGINT";
  public static final String BOOLEAN = "BOOLEAN";

  /**
   * Schema: default, table: MMA_OBJECT_META_TBL
   */
  public static final String MMA_OBJECT_META_TBL_NAME = "MMA_OBJECT_META_TBL";
  public static final String MMA_TBL_META_COL_UNIQUE_ID = "unique_id";
  public static final String MMA_TBL_META_COL_JOB_TYPE = "job_type";
  public static final String MMA_TBL_META_COL_OBJECT_TYPE = "object_type";
  public static final String MMA_TBL_META_COL_DB_NAME = "db_name";
  public static final String MMA_TBL_META_COL_OBJECT_NAME = "object_name";
  public static final String MMA_TBL_META_COL_IS_PARTITIONED = "is_partitioned";
  public static final String MMA_TBL_META_COL_MIGRATION_CONF = "migration_config";
  public static final String MMA_TBL_META_COL_STATUS = "status";
  public static final String MMA_TBL_META_COL_ATTEMPT_TIMES = "attempt_times";
  /**
   * Init value for column 'attempt_times'
   */
  public static final int MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES = 0;
  public static final String MMA_TBL_META_COL_LAST_MODIFIED_TIME = "last_modified_time";
  /**
   * N/A value for column 'last_modified_time'
   */
  public static final long MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME = -1L;
  public static final Map<String, String> MMA_TBL_META_COL_TO_TYPE;

  public static final String MMA_OBJ_RESTORE_TBL_NAME = "MMA_DATABASE_RESTORE";
  public static final String MMA_OBJ_RESTORE_COL_UNIQUE_ID = "unique_id";
  public static final String MMA_OBJ_RESTORE_COL_OBJECT_TYPE = "object_type";
  public static final String MMA_OBJ_RESTORE_COL_DB_NAME = "db_name";
  public static final String MMA_OBJ_RESTORE_COL_OBJECT_NAME = "object_name";
  public static final String MMA_OBJ_RESTORE_COL_JOB_CONFIG = "job_config";
  public static final String MMA_OBJ_RESTORE_COL_STATUS = "status";
  public static final String MMA_OBJ_RESTORE_COL_ATTEMPT_TIMES = "attempt_times";
  public static final int MMA_OBJ_RESTORE_INIT_ATTEMPT_TIMES = 0;
  public static final String MMA_OBJ_RESTORE_COL_LAST_MODIFIED_TIME = "last_modified_time";
  public static final long MMA_OBJ_RESTORE_INIT_LAST_MODIFIED_TIMESTAMP = -1L;
  public static final Map<String, String> MMA_OBJ_RESTORE_COL_TO_TYPE;

  public static final String MMA_PT_META_SCHEMA_NAME_FMT = "MMA_PT_META_DB_%s";
  public static final String MMA_PT_META_SCHEMA_NAME_PREFIX = "MMA_PT_META_DB_";
  public static final String MMA_PT_META_TBL_NAME_FMT = "MMA_PT_META_TBL_%s";
  public static final String MMA_PT_META_COL_UNIQUE_ID = "unique_id";
  public static final String MMA_PT_META_COL_JOB_TYPE = "job_type";
  public static final String MMA_PT_META_COL_PT_VALS = "pt_vals";
  public static final String MMA_PT_META_COL_STATUS = "status";
  public static final String MMA_PT_META_COL_ATTEMPT_TIMES = "attempt_times";
  public static final int MMA_PT_META_INIT_ATTEMPT_TIMES = 0;
  public static final String MMA_PT_META_COL_LAST_MODIFIED_TIME = "last_modified_time";
  public static final long MMA_PT_MEAT_NA_LAST_MODIFIED_TIME = -1L;
  public static final Map<String, String> MMA_PT_META_COL_TO_TYPE;

  public static final String MMA_OBJ_TEMPORARY_TBL_NAME = "MMA_DATABASE_TEMPORARY_TABLE";
  public static final String MMA_OBJ_TEMPORARY_COL_UNIQUE_ID = "unique_id";
  public static final String MMA_OBJ_TEMPORARY_COL_PROJECT = "db";
  public static final String MMA_OBJ_TEMPORARY_COL_TABLE = "tbl";
  public static final Map<String, String> MMA_OBJ_TEMPORARY_TBL_COL_TO_TYPE;

  static  {
    Map<String, String> temp = new LinkedHashMap<>();
    temp.put(MMA_TBL_META_COL_UNIQUE_ID, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_JOB_TYPE, VARCHAR_16);
    temp.put(MMA_TBL_META_COL_OBJECT_TYPE, VARCHAR_16);
    temp.put(MMA_TBL_META_COL_DB_NAME, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_OBJECT_NAME, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_IS_PARTITIONED, BOOLEAN);
    temp.put(MMA_TBL_META_COL_MIGRATION_CONF, TEXT);
    temp.put(MMA_TBL_META_COL_STATUS, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_ATTEMPT_TIMES, INT);
    temp.put(MMA_TBL_META_COL_LAST_MODIFIED_TIME, BIGINT);
    MMA_TBL_META_COL_TO_TYPE = Collections.unmodifiableMap(temp);

    temp = new LinkedHashMap<>();
    temp.put(MMA_OBJ_RESTORE_COL_UNIQUE_ID, VARCHAR_255);
    temp.put(MMA_OBJ_RESTORE_COL_OBJECT_TYPE, VARCHAR_16);
    temp.put(MMA_OBJ_RESTORE_COL_DB_NAME, VARCHAR_255);
    temp.put(MMA_OBJ_RESTORE_COL_OBJECT_NAME, VARCHAR_255);
    temp.put(MMA_OBJ_RESTORE_COL_JOB_CONFIG, TEXT);
    temp.put(MMA_OBJ_RESTORE_COL_STATUS, VARCHAR_255);
    temp.put(MMA_OBJ_RESTORE_COL_ATTEMPT_TIMES, INT);
    temp.put(MMA_OBJ_RESTORE_COL_LAST_MODIFIED_TIME, BIGINT);
    MMA_OBJ_RESTORE_COL_TO_TYPE = Collections.unmodifiableMap(temp);

    temp = new LinkedHashMap<>();
    temp.put(MMA_PT_META_COL_UNIQUE_ID, VARCHAR_255);
    temp.put(MMA_PT_META_COL_JOB_TYPE, VARCHAR_16);
    temp.put(MMA_PT_META_COL_PT_VALS, VARCHAR_255);
    temp.put(MMA_PT_META_COL_STATUS, VARCHAR_255);
    temp.put(MMA_PT_META_COL_ATTEMPT_TIMES, INT);
    temp.put(MMA_PT_META_COL_LAST_MODIFIED_TIME, BIGINT);
    MMA_PT_META_COL_TO_TYPE = Collections.unmodifiableMap(temp);

    temp = new LinkedHashMap<>();
    temp.put(MMA_OBJ_TEMPORARY_COL_UNIQUE_ID, VARCHAR_255);
    temp.put(MMA_OBJ_TEMPORARY_COL_PROJECT, VARCHAR_255);
    temp.put(MMA_OBJ_TEMPORARY_COL_TABLE, VARCHAR_255);
    MMA_OBJ_TEMPORARY_TBL_COL_TO_TYPE = Collections.unmodifiableMap(temp);
  }

  public static final String OSS_ROOT_FOLDER = "odps_mma/";
  public static final String EXPORT_OBJECT_ROOT_FOLDER = "odps_mma/export_objects/";
  public static final String EXPORT_FUNCTION_FOLDER = "functions/";
  public static final String EXPORT_RESOURCE_FOLDER = "resources/";
  public static final String EXPORT_TABLE_FOLDER = "tables/";
  public static final String EXPORT_TABLE_DATA_FOLDER = "data/";
  public static final String EXPORT_VIEW_FOLDER = "views/";
  public static final String EXPORT_META_FILE_NAME = "meta";
  public static final String EXPORT_COLUMN_TYPE_FILE_NAME = "column_type";
  public static final String EXPORT_PARTITION_SPEC_FILE_NAME = "partition_spec";
  public static final String EXPORT_OBJECT_FILE_NAME = "object";
}
