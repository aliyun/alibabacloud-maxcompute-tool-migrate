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

package com.aliyun.odps.datacarrier.taskscheduler.ui;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.TaskScheduler;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.ApiErrorHandler;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.JobApi;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.PublicKeyApi;
import com.aliyun.odps.datacarrier.taskscheduler.ui.tasks.TasksTab;
import com.aliyun.odps.datacarrier.taskscheduler.ui.utils.JettyUtils;

public class MmaUi extends WebUi {
  private static final Logger LOG = LogManager.getLogger(MmaUi.class);

  private static final String STATIC_RESOURCE_DIR =
      "com/aliyun/odps/datacarrier/taskscheduler/ui/static";

  public MmaUi(
      String basePath,
      MmaMetaManager mmaMetaManager,
      TaskScheduler taskScheduler) throws MmaException {
    super(basePath);
    TasksTab tasksTab = new TasksTab(this, "tasks", taskScheduler);
    attachTab(tasksTab);
    addStaticHandler(STATIC_RESOURCE_DIR, "/static");
    attachHandler(JettyUtils.createRedirectHandler("/", "/tasks/", basePath));
  }
}
