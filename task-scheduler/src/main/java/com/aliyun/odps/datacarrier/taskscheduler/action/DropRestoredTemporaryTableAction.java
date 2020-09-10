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

package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class DropRestoredTemporaryTableAction extends OdpsSqlAction {
  private static final Logger LOG = LogManager.getLogger(DropRestoredTemporaryTableAction.class);

  private AtomicInteger lineageTasksCounter;

  public DropRestoredTemporaryTableAction(String id,
                                          AtomicInteger lineageTasksCounter) {
    super(id);
    this.lineageTasksCounter = Objects.requireNonNull(lineageTasksCounter);
    this.lineageTasksCounter.incrementAndGet();
  }

  @Override
  String getSql() {
    int remainTasks = lineageTasksCounter.decrementAndGet();
    LOG.info("Action {}, remain lineage tasks {}", id, remainTasks);
    if (remainTasks == 0) {
      MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
      LOG.info("Action {}, drop restored temporary table {}.{}",
          id, tableMetaModel.databaseName, tableMetaModel.tableName);
      return OdpsSqlUtils.getDropTableStatement(tableMetaModel.databaseName, tableMetaModel.tableName);
    }
    return "";
  }

  @Override
  Map<String, String> getSettings() {
    // TODO: should be included in TableMigrationCongifg
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getDDLSettings();
  }
}
