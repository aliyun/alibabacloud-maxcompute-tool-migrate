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


import java.io.InputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Resource;
import com.aliyun.odps.mma.config.OssConfig;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;

public class McToOssResourceAction extends DefaultAction {

  private static final Logger LOG = LogManager.getLogger(McToOssResourceAction.class);
  private final OssConfig ossConfig;
  private final Odps odps;
  private final String project;
  private final String resourceName;
  private final String metafile;
  private final String datafile;
  private final ResourceMetaModel resourceMetaModel;

  public McToOssResourceAction(
      String id,
      Task task,
      ActionExecutionContext context,
      ResourceMetaModel resourceMetaModel,
      String project,
      String resourceName,
      Odps odps,
      OssConfig ossConfig,
      String metafile,
      String datafile
  ) {
    super(id, task, context);
    this.resourceMetaModel = resourceMetaModel;
    this.project = project;
    this.resourceName = resourceName;
    this.odps = odps;
    this.ossConfig = ossConfig;
    this.metafile = metafile;
    this.datafile = datafile;
  }

  @Override
  void handleResult(Object result) {
  }

  @Override
  public String getName() {
    return "Resource transmission";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object call() throws Exception {

    // translate meta info
    String content = GsonUtils.GSON.toJson(resourceMetaModel);
    LOG.info("Action: {}, resource info: {}", id, content);
    OssUtils.createFile(ossConfig, metafile, content);

    // translate data
    Resource resource = OdpsUtils.getResource(odps, project, resourceName);
    if (!Resource.Type.TABLE.equals(resourceMetaModel.getType())) {
      FileResource fileResource = (FileResource) resource;
      InputStream stream =
          odps.resources().getResourceAsStream(fileResource.getProject(), fileResource.getName());
      OssUtils.createFile(ossConfig, datafile, stream);
    }
    return null;
  }

}
