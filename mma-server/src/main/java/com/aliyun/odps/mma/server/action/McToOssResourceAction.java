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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Resource;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;

public class McToOssResourceAction extends DefaultAction {

  private static final Logger LOG = LogManager.getLogger(McToOssResourceAction.class);
  private final AbstractConfiguration config;
  private final OssConfig ossConfig;
  private final Odps odps;
  private final String metafile;
  private final String datafile;

  public McToOssResourceAction(
      String id,
      Odps odps,
      AbstractConfiguration config,
      OssConfig ossConfig,
      String metafile,
      String datafile,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.config = config;
    this.ossConfig = ossConfig;
    this.odps = odps;
    this.metafile = metafile;
    this.datafile = datafile;
  }


  @Override
  void handleResult(Object result) {}

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
    Resource resource = OdpsUtils.getResource(
        odps,
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME));
    if (StringUtils.isEmpty(resource.getName())) {
      LOG.error("Invalid resource name {} for task {}", resource.getName(), id);
      throw new MmaException("ERROR: Resource name is empty");
    }

    McResourceInfo resourceInfo = new McResourceInfo(resource);
    String content = GsonUtils.GSON.toJson(resourceInfo);
    LOG.info("Action: {}, resource info: {}", id, content);
    OssUtils.createFile(ossConfig, metafile, content);

    if(!Resource.Type.TABLE.equals(resourceInfo.getType())){
      FileResource fileResource = (FileResource) resource;
      InputStream stream = odps.resources().getResourceAsStream(fileResource.getProject(), fileResource.getName());
      OssUtils.createFile(ossConfig, datafile, stream);
    }
    return null;
  }

}
