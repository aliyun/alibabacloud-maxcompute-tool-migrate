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

package com.aliyun.odps.mma.server.ui.tasks;

import com.aliyun.odps.mma.server.JobScheduler;
import com.aliyun.odps.mma.server.ui.WebUi;
import com.aliyun.odps.mma.server.ui.WebUiTab;

public class TasksTab extends WebUiTab {

  public TasksTab(WebUi parent, String prefix, JobScheduler jobScheduler) {
    super(parent, prefix);
    attachPage(new AllTasksPage("", this, jobScheduler));
    attachPage(new TaskPage("task", this, jobScheduler));
    attachPage(new ActionPage("task/action", this, jobScheduler));
  }
}
