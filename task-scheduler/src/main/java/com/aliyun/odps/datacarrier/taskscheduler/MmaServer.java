package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventManager;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaSummaryEvent;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImpl;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProgress;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProvider;
import com.aliyun.odps.datacarrier.taskscheduler.ui.MmaUI;

public class MmaServer {
  private static final Logger LOG = LogManager.getLogger(MmaServer.class);

  /**
   * 1 hour
   */
  private static final int DEFAULT_REPORTING_INTERVAL_MS = 3600000;

  private volatile boolean keepRunning = true;

  private TaskScheduler taskScheduler;
  private MmaMetaManager mmaMetaManager = new MmaMetaManagerDbImpl(true);
  private MmaUI ui;

  private SummaryReportingThread summaryReportingThread;

  public MmaServer() throws MetaException, MmaException {
    TaskProvider taskProvider = new TaskProvider(mmaMetaManager);
    taskScheduler = new TaskScheduler(taskProvider);

    summaryReportingThread = new SummaryReportingThread();
    summaryReportingThread.setDaemon(true);
    summaryReportingThread.start();

    Map<String, String> uiConfig = MmaServerConfig.getInstance().getUIConfig();
    LOG.info("UI config: {}", uiConfig.toString());
    boolean uiEnabled = Boolean.parseBoolean(uiConfig.get(MmaServerConfig.MMA_UI_ENABLED));
    if (uiEnabled) {
      String host = uiConfig.get(MmaServerConfig.MMA_UI_HOST);
      int port = Integer.parseInt(uiConfig.get(MmaServerConfig.MMA_UI_PORT));
      int maxThreads = Integer.parseInt(uiConfig.get(MmaServerConfig.MMA_UI_THREADS_MAX));
      int minThreads = Integer.parseInt(uiConfig.get(MmaServerConfig.MMA_UI_THREADS_MIN));
      ui = new MmaUI("", mmaMetaManager, taskScheduler);
      ui.bind(host, port, maxThreads, minThreads);
    } else {
      LOG.info("MMA UI disabled");
    }
  }

  public void run() {
    taskScheduler.run();
  }


  public void shutdown() {
    keepRunning = false;
    try {
      summaryReportingThread.join();
    } catch (InterruptedException e) {
      LOG.warn(e);
    }

    try {
      taskScheduler.shutdown();
    } catch (Exception e) {
      LOG.warn(e);
    }

    try {
      mmaMetaManager.shutdown();
    } catch (Exception e) {
      LOG.warn(e);
    }

    try {
      ui.stop();
    } catch (Exception e) {
      LOG.warn(e);
    }
  }

  private class SummaryReportingThread extends Thread {
    private int reportingInterval = DEFAULT_REPORTING_INTERVAL_MS;

    public SummaryReportingThread() {
      super("SummaryReporter");
    }

    @Override
    public void run() {
      LOG.info("SummaryReportingThread starts");
      while (keepRunning) {
        try {
          int numPendingJobs = mmaMetaManager
              .listMigrationJobs(MmaMetaManager.JobStatus.PENDING, -1)
              .size();
          int numRunningJobs = mmaMetaManager
              .listMigrationJobs(MmaMetaManager.JobStatus.RUNNING, -1)
              .size();
          int numFailedJobs = mmaMetaManager
              .listMigrationJobs(MmaMetaManager.JobStatus.FAILED, -1)
              .size();
          int numSucceededJobs = mmaMetaManager
              .listMigrationJobs(MmaMetaManager.JobStatus.SUCCEEDED, -1)
              .size();

          Map<String, TaskProgress> taskToProgress = taskScheduler.summary();
          MmaSummaryEvent e = new MmaSummaryEvent(
              numPendingJobs, numRunningJobs, numFailedJobs, numSucceededJobs, taskToProgress);

          MmaEventManager.getInstance().send(e);
        } catch (MmaException e) {
          LOG.warn("Sending summary failed", e);
        }

        try {
          Thread.sleep(reportingInterval);
        } catch (InterruptedException ignore) {
        }
      }
    }
  }
}
