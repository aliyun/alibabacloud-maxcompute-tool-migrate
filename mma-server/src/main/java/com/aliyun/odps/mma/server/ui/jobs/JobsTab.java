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

import com.aliyun.odps.mma.server.job.JobManager;
import com.aliyun.odps.mma.server.ui.WebUi;
import com.aliyun.odps.mma.server.ui.WebUiTab;

public class JobsTab extends WebUiTab {

  public JobsTab(WebUi parent, String prefix, JobManager jobManager) {
    super(parent, prefix);
    attachPage(new AllJobsPage("", this, jobManager));
    attachPage(new JobPage("job", this, jobManager));
  }
}
