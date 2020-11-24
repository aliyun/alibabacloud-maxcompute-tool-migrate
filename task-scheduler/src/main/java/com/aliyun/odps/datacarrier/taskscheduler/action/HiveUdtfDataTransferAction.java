package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.HiveSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;

public class HiveUdtfDataTransferAction extends HiveSqlAction {

  private static final Logger LOG = LogManager.getLogger(HiveUdtfDataTransferAction.class);

  public HiveUdtfDataTransferAction(String id) {
    super(id);
    // Init default resourceMap
    resourceMap.put(Resource.HIVE_DATA_TRANSFER_JOB_RESOURCE, 1);
    resourceMap.put(Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE, 5);
  }

  @Override
  String getSql() {
    return HiveSqlUtils.getUdtfSql(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    Map<String, String> settings = new HashMap<>(
        MmaServerConfig
            .getInstance()
            .getHiveConfig()
            .getSourceTableSettings()
            .getMigrationSettings());

    if (resourceMap.containsKey(Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE)) {
      settings.put("mapreduce.job.running.map.limit",
                   resourceMap.get(Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE).toString());
    }
    return settings;
  }

  @Override
  public String getName() {
    return "Data transmission";
  }
}
