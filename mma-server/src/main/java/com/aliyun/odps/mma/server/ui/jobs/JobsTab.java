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
