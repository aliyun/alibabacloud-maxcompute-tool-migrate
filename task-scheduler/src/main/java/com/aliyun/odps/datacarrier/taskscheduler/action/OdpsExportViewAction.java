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

import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_VIEW_FOLDER;

public class OdpsExportViewAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportViewAction.class);

  private String taskName;
  private String viewText;

  public OdpsExportViewAction(String id, String taskName, String viewText) {
    super(id);
    this.taskName = taskName;
    this.viewText = viewText;
  }

  @Override
  public void doAction() {
    MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
    LOG.info("Task {}, export view {}.{}, viewText: {}", id, tableMetaModel.databaseName, tableMetaModel.tableName, viewText);
    String ossFileName = OssUtils.getOssPathToExportObject(taskName,
        EXPORT_VIEW_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        EXPORT_META_FILE_NAME);
    OssUtils.createFile(ossFileName, viewText);
  }

  @Override
  public String getName() {
    return "View exporting";
  }
}
