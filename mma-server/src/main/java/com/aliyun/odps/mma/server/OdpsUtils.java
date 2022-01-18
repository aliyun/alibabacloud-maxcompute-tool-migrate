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

package com.aliyun.odps.mma.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.ArchiveResource;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.PyResource;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.config.MmaConfig;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.utils.StringUtils;

public class OdpsUtils {
  private static final Logger LOG = LogManager.getLogger(OdpsUtils.class);

  public static Odps getOdps(String id, String key, String endpoint, String project) {
    AliyunAccount aliyunAccount = new AliyunAccount(id, key);
    Odps odps = new Odps(aliyunAccount);
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(project);
    odps.setUserAgent("MMA");
    return odps;
  }

  private static Odps getOdps(MmaConfig.OdpsConfig odpsConfig) {
    AliyunAccount aliyunAccount = new AliyunAccount(
        odpsConfig.getAccessId(), odpsConfig.getAccessKey());
    Odps odps = new Odps(aliyunAccount);
    odps.setEndpoint(odpsConfig.getEndpoint());
    odps.setDefaultProject(odpsConfig.getProjectName());
    odps.setUserAgent("MMA");
    return odps;
  }

  public static Table getTable(
      MmaConfig.OdpsConfig odpsConfig, String databaseName, String tableName) {
    Odps odps = getOdps(odpsConfig);
    try {
      if (odps.tables().exists(databaseName, tableName)) {
        return odps.tables().get(databaseName, tableName);
      }
    } catch (OdpsException e) {
      LOG.info("Get table {}.{} failed", databaseName, tableName, e);
    }
    return null;
  }

  public static Resource getResource(ResourceMetaModel resourceMetaModel) throws MmaException {
    Resource resource;
    switch (resourceMetaModel.getType()) {
      case ARCHIVE:
        resource = new ArchiveResource();
        break;
      case PY:
        resource = new PyResource();
        break;
      case JAR:
        resource = new JarResource();
        break;
      case FILE:
        resource = new FileResource();
        break;
      case TABLE:
        PartitionSpec spec = null;
        if (!StringUtils.isNullOrEmpty(resourceMetaModel.getPartitionSpec())) {
          spec = new PartitionSpec(resourceMetaModel.getPartitionSpec());
        }
        resource = new TableResource(resourceMetaModel.getTableName(), null, spec);
        break;
      default:
        throw new MmaException("Unknown resource type: " + resourceMetaModel.getType());
    }

    resource.setName(resourceMetaModel.getAlias());
    resource.setComment(resourceMetaModel.getComment());
    return resource;
  }

  public static Resource getResource(Odps odps, String project, String name)
      throws OdpsException, MmaException {
    if (odps.resources().exists(project, name)) {
      return odps.resources().get(project, name);
    }
    throw new MmaException("Resource " + name + " not exists");
  }

  public static void createFunction(
      Odps odps,
      String project,
      FunctionMetaModel functionMetaModel,
      boolean isUpdate) throws OdpsException {
    LOG.info("Create function {}.{} class {}, resources {}, update {}",
             project,
             functionMetaModel.getFunctionName(),
             functionMetaModel.getClassName(),
             functionMetaModel.getUseList(),
             isUpdate);
    Function function = new Function();
    function.setName(functionMetaModel.getFunctionName());
    function.setClassPath(functionMetaModel.getClassName());
    function.setResources(functionMetaModel.getUseList());
    if (odps.functions().exists(project, function.getName()) && isUpdate) {
      odps.functions().update(project, function);
    } else {
      odps.functions().create(project, function);
    }
  }

  public static void addFileResource(
      Odps odps,
      String projectName,
      FileResource resource,
      String absoluteLocalFilePath,
      boolean isUpdate,
      boolean deleteLocalFile) throws OdpsException {
    File file = new File(absoluteLocalFilePath);
    if (file.exists()) {
      try (FileInputStream inputStream = new FileInputStream(file)) {
        boolean exists = odps.resources().exists(resource.getName());
        if (exists && isUpdate) {
          odps.resources().update(projectName, resource, inputStream);
        } else if (!exists) {
          odps.resources().create(projectName, resource, inputStream);
        }
      } catch (IOException e) {
        throw new OdpsException("Add resource " + resource.getName() + " to " + projectName + " failed cause upload file fail.", e);
      }

      if (deleteLocalFile) {
        boolean deleted = file.delete();
        if (!deleted) {
          LOG.warn("Failed to delete temp file: " + absoluteLocalFilePath);
        }
      }
    } else {
      throw new OdpsException("File not found:" + file.getAbsolutePath() + " when add resource " + resource.getName() + " to " + projectName);
    }
  }

  public static void addTableResource(
      Odps odps,
      String projectName,
      TableResource resource,
      boolean isUpdate) throws OdpsException {
    boolean exists = odps.resources().exists(resource.getName());
    if (exists && isUpdate) {
      odps.resources().update(projectName, resource);
    } else if (!exists) {
      odps.resources().create(projectName, resource);
    }
  }
}
