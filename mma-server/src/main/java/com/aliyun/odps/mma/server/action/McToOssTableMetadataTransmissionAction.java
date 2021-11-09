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


import com.aliyun.odps.mma.config.OssConfig;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;

public class McToOssTableMetadataTransmissionAction extends DefaultAction {

  private OssConfig ossConfig;
  private TableMetaModel tableMetaModel;

  public McToOssTableMetadataTransmissionAction(
      String id,
      TableMetaModel ossTableMetaModel,
      OssConfig ossConfig,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.ossConfig = ossConfig;
    this.tableMetaModel = ossTableMetaModel;
  }

  @Override
  public String getName() {
    return "Table metadata transmission";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object call() {
    OssUtils.createFile(ossConfig,
                        tableMetaModel.getLocation(),
                        GsonUtils.GSON.toJson(tableMetaModel));
    return null;
  }

  @Override
  void handleResult(Object result) {
  }
}
