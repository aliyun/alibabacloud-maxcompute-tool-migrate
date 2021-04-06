/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.mma.server.action;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import com.aliyun.odps.ArchiveResource;
//import com.aliyun.odps.FileResource;
//import com.aliyun.odps.JarResource;
//import com.aliyun.odps.PartitionSpec;
//import com.aliyun.odps.PyResource;
//import com.aliyun.odps.Resource;
//import com.aliyun.odps.TableResource;
//import com.aliyun.odps.datacarrier.taskscheduler.Constants;
//import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
//import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
//import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
//import com.aliyun.odps.utils.StringUtils;
//
//
//public class OdpsRestoreResourceAction extends OdpsRestoreAction {
//
//  private static final Logger LOG = LogManager.getLogger(OdpsRestoreResourceAction.class);
//
//
//  public OdpsRestoreResourceAction(
//      String id,
//      MmaConfig.ObjectRestoreConfig restoreConfig) {
//    super(id, restoreConfig);
//  }
//
//  @Override
//  public void restore() throws Exception {
//    String metaFileName = getRestoredFilePath(
//        Constants.EXPORT_RESOURCE_FOLDER, Constants.EXPORT_META_FILE_NAME);
//    String content = OssUtils.readFile(restoreConfig.getOssConfig(), metaFileName);
//    OdpsResourceInfo resourceInfo = GsonUtils.getFullConfigGson().fromJson(
//        content, OdpsResourceInfo.class);
//    Resource resource;
//    switch (resourceInfo.getType()) {
//      case ARCHIVE:
//        resource = new ArchiveResource();
//        break;
//      case PY:
//        resource = new PyResource();
//        break;
//      case JAR:
//        resource = new JarResource();
//        break;
//      case FILE:
//        resource = new FileResource();
//        break;
//      case TABLE:
//        PartitionSpec spec = null;
//        String partitionSpec = resourceInfo.getPartitionSpec();
//        if (!StringUtils.isNullOrEmpty(partitionSpec)) {
//          spec = new PartitionSpec(partitionSpec);
//        }
//        resource = new TableResource(resourceInfo.getTableName(), null, spec);
//        break;
//      default:
//        throw new MmaException("Unknown resource type: " + resourceInfo.getType());
//    }
//    resource.setName(resourceInfo.getAlias());
//    resource.setComment(resourceInfo.getComment());
//    if (Resource.Type.TABLE.equals(resourceInfo.getType())) {
//      OdpsUtils.addTableResource(
//          restoreConfig.getOdpsConfig(),
//          getDestinationProject(),
//          (TableResource) resource,
//          isUpdate());
//    } else {
//      String fileName = getRestoredFilePath(
//          Constants.EXPORT_RESOURCE_FOLDER, Constants.EXPORT_OBJECT_FILE_NAME);
//      String localFilePath = OssUtils.downloadFile(
//          restoreConfig.getOssConfig(), actionExecutionContext.getJobId(), fileName);
//
//      OdpsUtils.addFileResource(
//          restoreConfig.getOdpsConfig(),
//          getDestinationProject(),
//          (FileResource) resource,
//          localFilePath,
//          isUpdate(),
//          true);
//    }
//
//    LOG.info("Restore resource {} succeed", content);
//  }
//
//  @Override
//  public String getName() {
//    return "Resource restoration";
//  }
//}
