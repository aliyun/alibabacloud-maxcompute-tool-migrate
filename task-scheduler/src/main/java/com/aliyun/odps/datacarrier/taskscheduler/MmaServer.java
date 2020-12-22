package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventManager;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaSummaryEvent;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSourceFactory;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager.MigrationStatus;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImpl;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProgress;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProvider;
import com.aliyun.odps.datacarrier.taskscheduler.ui.MmaUI;


public class MmaServer {
  private static final Logger LOG = LogManager.getLogger(MmaServer.class);

  private static final int DEFAULT_REPORTING_INTERVAL_MS = 60 * 60 * 1000;

  private volatile boolean keepRunning = true;

  private TaskScheduler taskScheduler;
  private MmaMetaManager mmaMetaManager;
  private MmaUI ui;

  private SummaryReportingThread summaryReportingThread;

  public MmaServer() throws MetaException, MmaException {
    mmaMetaManager = new MmaMetaManagerDbImpl(MetaSourceFactory.getMetaSource(), true);

    TaskProvider taskProvider = new TaskProvider(mmaMetaManager);
    taskScheduler = new TaskScheduler(taskProvider);

    summaryReportingThread = new SummaryReportingThread();
    summaryReportingThread.start();

    boolean uiEnabled = Boolean.parseBoolean(MmaServerConfig.getInstance().getUIConfig().get(MmaServerConfig.MMA_UI_ENABLED));
    if (uiEnabled) {
      // Start Mma UI
      String host =
          MmaServerConfig.getInstance().getUIConfig().get(MmaServerConfig.MMA_UI_HOST);
      int port = Integer.valueOf(
          MmaServerConfig.getInstance().getUIConfig().get(MmaServerConfig.MMA_UI_PORT));
      int maxThreads = Integer.valueOf(
          MmaServerConfig.getInstance().getUIConfig().get(MmaServerConfig.MMA_UI_MAX_THREADS));
      int minThreads = Integer.valueOf(
          MmaServerConfig.getInstance().getUIConfig().get(MmaServerConfig.MMA_UI_MIN_THREADS));
      ui = new MmaUI("", taskScheduler);
      ui.bind(host, port, maxThreads, minThreads);
    } else {
      LOG.info("MMA UI disabled");
    }
  }

  public void run(){
    taskScheduler.run();
  }

  public void shutdown() {
    keepRunning = false;
    try {
      summaryReportingThread.join();
    } catch (InterruptedException ignore) {
    }

    taskScheduler.shutdown();

    try {
      mmaMetaManager.shutdown();
    } catch (Exception ignore) {
    }

    try {
      ui.stop();
    } catch (Exception ignore) {
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
              .listMigrationJobs(MigrationStatus.PENDING, -1)
              .size();
          int numRunningJobs = mmaMetaManager
              .listMigrationJobs(MigrationStatus.RUNNING, -1)
              .size();
          int numFailedJobs = mmaMetaManager
              .listMigrationJobs(MigrationStatus.FAILED, -1)
              .size();
          int numSucceededJobs = mmaMetaManager
              .listMigrationJobs(MigrationStatus.SUCCEEDED, -1)
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
