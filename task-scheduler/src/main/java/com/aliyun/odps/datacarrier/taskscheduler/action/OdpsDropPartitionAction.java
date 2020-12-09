package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;

public class OdpsDropPartitionAction extends OdpsSqlAction {

  public OdpsDropPartitionAction(String id) {
    super(id);
    resourceMap.put(Resource.MC_METADATA_OPERATION_RESOURCE, 1);
  }

  @Override
  String getSql() {
    return OdpsSqlUtils.getDropPartitionStatement(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    // TODO: should be included in TableMigrationCongifg
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getDDLSettings();
  }

  @Override
  public String getName() {
    return "Partition deletion";
  }
}
