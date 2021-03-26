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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsFunctionInfo;

public class OdpsUtils {
  private static final Logger LOG = LogManager.getLogger(OdpsUtils.class);

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

  public static Function getFunction(
      MmaConfig.OdpsConfig odpsConfig, String databaseName, String functionName) {
    Odps odps = getOdps(odpsConfig);
    try {
      if (odps.functions().exists(databaseName, functionName)) {
        return odps.functions().get(databaseName, functionName);
      }
    } catch (OdpsException e) {
      LOG.error("Get function {}.{} failed", databaseName, functionName, e);
    }
    return null;
  }

  public static void createFunction(
      MmaConfig.OdpsConfig odpsConfig,
      String project,
      OdpsFunctionInfo functionInfo,
      boolean isUpdate) throws OdpsException {
    Odps odps = getOdps(odpsConfig);
    Function function = new Function();
    function.setName(functionInfo.getFunctionName());
    function.setClassPath(functionInfo.getClassName());
    function.setResources(functionInfo.getUseList());
    LOG.info("Create function {}.{} class {}, resources {}, update {}",
             project,
             functionInfo.getFunctionName(),
             functionInfo.getClassName(),
             functionInfo.getUseList(),
             isUpdate);
    if (odps.functions().exists(project, functionInfo.getFunctionName()) && isUpdate) {
      odps.functions().update(project, function);
    } else {
      odps.functions().create(project, function);
    }
  }

  public static Resource getResource(
      MmaConfig.OdpsConfig odpsConfig, String databaseName, String resourceName) {
    Odps odps = getOdps(odpsConfig);
    try {
      if (odps.resources().exists(databaseName, resourceName)) {
        return odps.resources().get(databaseName, resourceName);
      }
    } catch (OdpsException e) {
      LOG.info("Get resource {}.{} failed", databaseName, resourceName, e);
    }
    return null;
  }

  public static void addFileResource(
      MmaConfig.OdpsConfig odpsConfig,
      String projectName,
      FileResource resource,
      String absoluteLocalFilePath,
      boolean isUpdate,
      boolean deleteLocalFile) throws OdpsException {
    Odps odps = getOdps(odpsConfig);
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
      MmaConfig.OdpsConfig odpsConfig,
      String projectName,
      TableResource resource,
      boolean isUpdate) throws OdpsException {
    Odps odps = getOdps(odpsConfig);
    boolean exists = odps.resources().exists(resource.getName());
    if (exists && isUpdate) {
      odps.resources().update(projectName, resource);
    } else if (!exists) {
      odps.resources().create(projectName, resource);
    }
  }
}
