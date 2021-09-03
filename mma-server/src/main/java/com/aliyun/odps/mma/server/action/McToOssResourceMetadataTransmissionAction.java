/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
