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

import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.h2.util.StringUtils;

public class OdpsDropTableAction extends OdpsSqlAction {

  private String db;
  private String tbl;
  private boolean isView = false;

  public OdpsDropTableAction(String id) {
    super(id);
  }

  public OdpsDropTableAction(String id, boolean isView) {
    this(id);
    this.isView = isView;
  }

  public OdpsDropTableAction(String id, String db, String tbl, boolean isView) {
    this(id);
    this.db = db;
    this.tbl = tbl;
    this.isView = isView;
  }

  @Override
  String getSql() {
    if (StringUtils.isNullOrEmpty(db) || StringUtils.isNullOrEmpty(tbl)) {
      MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
      db = tableMetaModel.odpsProjectName;
      tbl = tableMetaModel.odpsTableName;
    }
    if (isView) {
      return OdpsSqlUtils.getDropViewStatement(db, tbl);
    }
    return OdpsSqlUtils.getDropTableStatement(db, tbl);
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
