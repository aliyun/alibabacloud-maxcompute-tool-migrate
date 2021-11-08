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

import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;

public class McToOssFunctionAction extends DefaultAction {

  private static final Logger LOG = LogManager.getLogger(McToOssFunctionAction.class);
  private final OssConfig ossConfig;
  private final String metafile;
  private final FunctionMetaModel functionMetaModel;

  public McToOssFunctionAction(
      String id,
      Task task,
      ActionExecutionContext context,
      FunctionMetaModel functionMetaModel,
      OssConfig ossConfig,
      String metafile ) {
    super(id, task, context);
    this.functionMetaModel = functionMetaModel;
    this.ossConfig = ossConfig;
    this.metafile = metafile;
  }


  @Override
  void handleResult(Object result) {}

  @Override
  public String getName() {
    return "resource transmission";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object call() throws Exception {
    String content = GsonUtils.GSON.toJson(functionMetaModel);
    LOG.info("Action: {}, function info: {}", id, content);
    OssUtils.createFile(ossConfig, metafile, content);
    return null;
  }
}
