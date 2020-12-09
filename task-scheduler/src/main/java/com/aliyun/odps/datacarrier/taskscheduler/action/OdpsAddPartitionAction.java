package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;

public class OdpsAddPartitionAction extends OdpsSqlAction {

  public OdpsAddPartitionAction(String id) {
    super(id);
    resourceMap.put(Resource.MC_METADATA_OPERATION_RESOURCE, 1);
  }

  @Override
  String getSql() {
    return OdpsSqlUtils.getAddPartitionStatement(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getDDLSettings();
  }

  @Override
  public String getName() {
    return "Partition creation";
  }
}
