package com.aliyun.odps.mma.server.action;

import com.aliyun.odps.Resource;
import com.aliyun.odps.mma.server.task.Task;

public class McToOssResourceMetadataTransmissionAction extends AbstractAction {

  private Resource resource;

  public McToOssResourceMetadataTransmissionAction(
      String id,
      Resource resource,
      String ossAccessKeyId,
      String ossAccessKeySecret,
      String ossRoleArn,
      String ossBucket,
      String ossEndpoint,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.resource = resource;
  }

  @Override
  void executeInternal() {

  }

  @Override
  void handleResult(Object result) {
  }

  @Override
  public String getName() {
    return "Resource metadata transmission";
  }

  @Override
  public Object getResult() {
    return null;
  }
}
