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

package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.List;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;

/**
 * Should support Multi-threading
 */
public class ActionExecutionContext {

  private TableMetaModel tableMetaModel = null;
  private List<List<String>> sourceVerificationResult = null;
  private List<List<String>> destVerificationResult = null;

  public TableMetaModel getTableMetaModel() {
    return tableMetaModel;
  }

  public void setTableMetaModel(TableMetaModel tableMetaModel) {
    this.tableMetaModel = tableMetaModel;
  }

  public List<List<String>> getSourceVerificationResult() {
    return sourceVerificationResult;
  }

  public void setSourceVerificationResult(List<List<String>> rows) {
    sourceVerificationResult = rows;
  }

  public List<List<String>> getDestVerificationResult() {
    return destVerificationResult;
  }

  public void setDestVerificationResult(List<List<String>> rows) {
    destVerificationResult = rows;
  }
}
