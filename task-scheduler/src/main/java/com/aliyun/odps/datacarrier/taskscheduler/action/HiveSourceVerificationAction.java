package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.HiveSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;

public class HiveSourceVerificationAction extends HiveSqlAction {

  private static final Logger LOG = LogManager.getLogger(HiveSourceVerificationAction.class);

  public HiveSourceVerificationAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    return HiveSqlUtils.getVerifySql(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    return MmaServerConfig
        .getInstance()
        .getHiveConfig()
        .getSourceTableSettings()
        .getVerifySettings();
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      List<List<String>> rows = future.get();

      if (Level.DEBUG.equals(LOG.getLevel())) {
        for (List<String> row : rows) {
          LOG.debug("Source verification result: {}", row);
        }
      }

      actionExecutionContext.setSourceVerificationResult(rows);
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id,
                ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }
}
