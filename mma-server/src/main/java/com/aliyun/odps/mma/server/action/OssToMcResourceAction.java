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

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.mma.config.OssConfig;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.task.Task;

public class OssToMcResourceAction extends DefaultAction {

  private static final Logger LOG = LogManager.getLogger(OssToMcResourceAction.class);
  private final OssConfig ossConfig;
  private final ResourceMetaModel resourceMetaModel;
  private final String datafile;
  private final Odps odps;
  private final String project;
  private final boolean update;

  public OssToMcResourceAction(
      String id,
      OssConfig ossConfig,
      ResourceMetaModel resourceMetaModel,
      String datafile,
      Odps odps,
      String project,
      boolean update,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.ossConfig = ossConfig;
    this.resourceMetaModel = resourceMetaModel;
    this.datafile = datafile;
    this.odps = odps;
    this.project = project;
    this.update = update;
  }

  @Override
  void handleResult(Object result) {
  }

  @Override
  public String getName() {
    return "resource restoration";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object call() throws Exception {

    // restore meta model
    Resource resource = OdpsUtils.getResource(resourceMetaModel);

    // restore meta + data
    if (Resource.Type.TABLE.equals(resource.getType())) {
      OdpsUtils.addTableResource(odps, project, (TableResource) resource, update);
    } else {
      if (!OssUtils.exists(ossConfig, datafile)) {
        throw new MmaException(String.format("ActionId: %s, OSS file %s not found", id, datafile));
      }
      // todo jobid
      String localFilePath = OssUtils.downloadFile(ossConfig, id, datafile);
      OdpsUtils.addFileResource(odps, project,
                                (FileResource) resource, localFilePath,
                                update, true);
    }
    LOG.info("Restore resource {} succeed", resource.getName());

    return null;
  }

}
