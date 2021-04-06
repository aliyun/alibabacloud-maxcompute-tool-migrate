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
