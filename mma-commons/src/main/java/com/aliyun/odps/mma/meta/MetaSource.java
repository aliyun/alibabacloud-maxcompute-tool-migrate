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

import java.util.List;

import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;

public interface MetaSource {

  /**
   * Check database existence
   *
   * @param databaseName Database name
   * @return True if the database exists, else false
   * @throws Exception
   */
  boolean hasDatabase(String databaseName) throws Exception;

  /**
   * Check table existence
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return True if the table exists, else false
   * @throws Exception
   */
  boolean hasTable(String databaseName, String tableName) throws Exception;

  /**
   * Check partition existence
   * @param databaseName Database name
   * @param tableName Table name
   * @param partitionValues partition values
   * @return True if the partition exists, else false
   * @throws Exception
   */
  boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception;

  /**
   * Get database list
   *
   * @return List of database
   * @throws Exception
   */
  List<String> listDatabases() throws Exception;

  /**
   * Get table names in given database
   *
   * @param databaseName Database name
   * @return List of table names, include views
   * @throws Exception
   */
  List<String> listTables(String databaseName) throws Exception;

  List<String> listResources(String databaseName) throws Exception;

  List<String> listFunctions(String databaseName) throws Exception;

  /**
   * Get partition list of specified table
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return List of partition values of specified table
   * @throws Exception
   */
  List<List<String>> listPartitions(String databaseName,
                                    String tableName) throws Exception;

  /**
   * Get metadata of specified table
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return Metadata of specified table
   * @throws Exception
   */
  TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception;

  /**
   * Get metadata of specified table
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return Metadata of specified table, partition metadata not included
   * @throws Exception
   */
  TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                  String tableName) throws Exception;

  /**
   * Get metadata of specified partition
   *
   * @param databaseName    Database name
   * @param tableName       Table name
   * @param partitionValues Partition values
   * @return Metadata of specified partition
   * @throws Exception
   */
  PartitionMetaModel getPartitionMeta(String databaseName,
                                      String tableName,
                                      List<String> partitionValues) throws Exception;


  /**
   * Get metadata of resource
   *
   * @param databaseName    Database name
   * @param resourceName    Resource name
   * @return Metadata of specified partition
   * @throws Exception
   */
  ResourceMetaModel getResourceMeta(String databaseName,
                                    String resourceName) throws Exception;


  /**
   * Get metadata of function
   *
   * @param databaseName    Database name
   * @param functionName    Function name
   * @return Metadata of specified partition
   * @throws Exception
   */
  FunctionMetaModel getFunctionMeta(String databaseName,
                                    String functionName) throws Exception;


  List<ObjectType> getSupportedObjectTypes();

  /**
   * Shutdown
   */
  void shutdown();
}
