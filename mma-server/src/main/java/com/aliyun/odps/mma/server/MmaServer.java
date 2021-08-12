package com.aliyun.odps.mma.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.server.job.JobManager;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.ui.MmaApi;
import com.aliyun.odps.mma.server.ui.MmaUi;

public class MmaServer {
  private static final Logger LOG = LogManager.getLogger(MmaServer.class);

  private JobManager jobManager;
  private JobScheduler jobScheduler;
  private MmaUi ui;
  private MmaApi api;

  public MmaServer() throws Exception {
    MetaManager metaManager = new MetaManager();
    MetaSourceFactory metaSourceFactory = new MetaSourceFactory();
    jobManager = new JobManager(metaManager, metaSourceFactory);
    jobScheduler = new JobScheduler();

    // Start API. This step must be the first step of initialization since the start script waits
    // only 5 seconds before checking the port of API, by which the start script determines if MMA
    // server starts successfully.
    boolean apiEnabled = Boolean.parseBoolean(MmaServerConfiguration.getInstance().getOrDefault(
        MmaServerConfiguration.API_ENABLED,
        MmaServerConfiguration.API_ENABLED_DEFAULT_VALUE));
    if (apiEnabled) {
      String host = MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.API_HOST,
          MmaServerConfiguration.API_HOST_DEFAULT_VALUE);
      int port = Integer.parseInt(MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.API_PORT,
          MmaServerConfiguration.API_PORT_DEFAULT_VALUE));
      int maxThreads = Integer.parseInt(MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.API_THREADS_MAX,
          MmaServerConfiguration.API_THREADS_MAX_DEFAULT_VALUE));
      int minThreads = Integer.parseInt(MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.API_THREADS_MIN,
          MmaServerConfiguration.API_THREADS_MIN_DEFAULT_VALUE));
      api = new MmaApi("", jobManager, jobScheduler);
      api.bind(host, port, maxThreads, minThreads);
      LOG.info("MMA API is running at " + host + ":" + port);
    } else {
      LOG.info("MMA API is disabled");
    }

    // Start Web UI
    boolean uiEnabled = Boolean.parseBoolean(MmaServerConfiguration.getInstance().getOrDefault(
        MmaServerConfiguration.UI_ENABLED,
        MmaServerConfiguration.UI_ENABLED_DEFAULT_VALUE));
    if (uiEnabled) {
      String host = MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.UI_HOST,
          MmaServerConfiguration.UI_HOST_DEFAULT_VALUE);
      int port = Integer.parseInt(MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.UI_PORT,
          MmaServerConfiguration.UI_PORT_DEFAULT_VALUE));
      int maxThreads = Integer.parseInt(MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.UI_THREADS_MAX,
          MmaServerConfiguration.UI_THREADS_MAX_DEFAULT_VALUE));
      int minThreads = Integer.parseInt(MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.UI_THREADS_MIN,
          MmaServerConfiguration.UI_THREADS_MIN_DEFAULT_VALUE));
      ui = new MmaUi("", jobManager, jobScheduler);
      ui.bind(host, port, maxThreads, minThreads);
      LOG.info("MMA UI is running at " + host + ":" + port);
    } else {
      LOG.info("MMA UI is disabled");
    }

    // Recover from unexpected process termination
    jobManager.recover();
    for (Job job : jobManager.listJobsByStatus(JobStatus.PENDING)) {
      jobScheduler.schedule(job);
    }
  }

  public void run() {
    jobScheduler.run();
  }

  public void run(JobConfiguration config) throws Exception {
    jobScheduler = new JobScheduler(true);

    config.validate();

    String jobId;
    jobId = jobManager.addJob(config);
    Job job;
    job = jobManager.getJobById(jobId);
    jobScheduler.schedule(job);
    jobScheduler.run();
  }

  public void shutdown() {
    try {
      jobScheduler.shutdown();
    } catch (Exception e) {
      LOG.warn(e);
    }

    try {
      ui.stop();
    } catch (Exception e) {
      LOG.error(e);
    }

    try {
      api.stop();
    } catch (Exception e) {
      LOG.error(e);
    }
  }
}
