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

import static j2html.TagCreator.h4;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.server.JobScheduler;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.ui.WebUiPage;
import com.aliyun.odps.mma.server.ui.WebUiTab;
import com.aliyun.odps.mma.server.ui.utils.UiUtils;
import j2html.tags.DomContent;

public class AllTasksPage extends WebUiPage {

  private static final Logger LOG = LogManager.getLogger(AllTasksPage.class);

  private WebUiTab parent;
  private JobScheduler jobScheduler;

  AllTasksPage(String prefix, WebUiTab parent, JobScheduler jobScheduler) {
    super(prefix);
    this.parent = Objects.requireNonNull(parent);
    this.jobScheduler = Objects.requireNonNull(jobScheduler);
  }

  @Override
  public String render(HttpServletRequest request) {

    List<DomContent> content = new LinkedList<>();

    List<Task> runningTasks = jobScheduler.getRunningTasks();
    content.add(
        h4("Running Tasks (" + runningTasks.size() + ")").withId("running")
    );
    String parameterPath = String.join("/", parent.getBasePath(), parent.getPrefix());
    content.add(
        UiUtils.tasksTable(
            parameterPath,
            null,
            null,
            request,
            "running",
            "runningTask",
            runningTasks,
            LOG
        )
    );
    List<Task> failedTasks = jobScheduler.getFailedTasks();
    content.add(
        h4("Failed Tasks (" + failedTasks.size() + ")").withId("failed")
    );
    content.add(
        UiUtils.tasksTable(
            parameterPath,
            null,
            null,
            request,
            "failed",
            "failedTask",
            failedTasks,
            LOG
        )
    );
    return UiUtils.basicMmaPage("Tasks", content, parent);
  }
}
