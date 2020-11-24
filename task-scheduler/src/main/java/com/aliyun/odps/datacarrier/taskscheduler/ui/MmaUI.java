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

import com.aliyun.odps.datacarrier.taskscheduler.TaskScheduler;
import com.aliyun.odps.datacarrier.taskscheduler.ui.tasks.TasksTab;
import com.aliyun.odps.datacarrier.taskscheduler.ui.utils.JettyUtils;

public class MmaUI extends WebUI {
  private static final Logger LOG = LogManager.getLogger(MmaUI.class);

  private static final String STATIC_RESOURCE_DIR =
      "com/aliyun/odps/datacarrier/taskscheduler/ui/static";

  public MmaUI(int port, String basePath, TaskScheduler taskScheduler) {
    super(port, basePath);
    TasksTab tasksTab = new TasksTab(this, "tasks", taskScheduler);
    attachTab(tasksTab);
    addStaticHandler(STATIC_RESOURCE_DIR, "/static");
    attachHandler(JettyUtils.createRedirectHandler("/", "/tasks/", basePath));
  }
}
