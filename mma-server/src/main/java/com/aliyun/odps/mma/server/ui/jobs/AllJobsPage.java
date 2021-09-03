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

package com.aliyun.odps.mma.server.ui.jobs;

import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.server.job.JobManager;
import com.aliyun.odps.mma.server.ui.WebUiPage;
import com.aliyun.odps.mma.server.ui.WebUiTab;
import com.aliyun.odps.mma.server.ui.utils.UiUtils;
import j2html.tags.DomContent;

public class AllJobsPage extends WebUiPage {
  private static final Logger LOG = LogManager.getLogger(AllJobsPage.class);

  private WebUiTab parent;
  private JobManager jobManager;

  AllJobsPage(String prefix, WebUiTab parent, JobManager jobManager) {
    super(prefix);
    this.parent = parent;
    this.jobManager = jobManager;
  }

  @Override
  public String render(HttpServletRequest request) {
    List<DomContent> content = new LinkedList<>();
    List<Job> jobs = jobManager.listJobs();

    String parameterPath = String.join("/", parent.getBasePath(), parent.getPrefix());
    content.add(
        UiUtils.jobsTable(
            parameterPath,
            request,
            "jobs",
            "jobs",
            null,
            null,
            jobs,
            LOG)
    );

    return UiUtils.basicMmaPage("Jobs", content, parent);
  }
}
