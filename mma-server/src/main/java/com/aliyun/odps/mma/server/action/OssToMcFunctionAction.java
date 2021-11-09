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
 *
 */

package com.aliyun.odps.mma.server.action;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.task.Task;

public class OssToMcFunctionAction extends DefaultAction {

  private static final Logger LOG = LogManager.getLogger(OssToMcFunctionAction.class);
  private final FunctionMetaModel functionMetaModel;
  private final Odps odps;
  private final boolean update;

  public OssToMcFunctionAction(
      String id,
      FunctionMetaModel functionMetaModel,
      Odps odps,
      boolean update,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.functionMetaModel = functionMetaModel;
    this.odps = odps;
    this.update = update;
  }


  @Override
  void handleResult(Object result) {
  }

  @Override
  public String getName() {
    return "function restoration";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object call() throws Exception {
    //todo use odpsconfig
    OdpsUtils.createFunction(odps, odps.getDefaultProject(), functionMetaModel, update);
    LOG.info("Restore function {} succeed", functionMetaModel.getFunctionName());
    return null;
  }

}
