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

package com.aliyun.odps.datacarrier.taskscheduler.config;

import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaExceptionFactory;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.h2.util.StringUtils;

public class ProjectRestoreJobInputsV1 implements Config {

  @Expose
  @SerializedName("JobId")
  String jobId;

  @Expose
  @SerializedName("DataSource")
  DataSource dataSource;

  @Expose
  @SerializedName("SourceProjectName")
  String sourceProjectName;

  @Expose
  @SerializedName("DestProjectName")
  String destProjectName;

  @Expose
  @SerializedName("ObjectTypes")
  List<MmaConfig.ObjectType> objectTypes;

  @Expose
  @SerializedName("BackupName")
  String backupName;

  @Expose
  @SerializedName("UpdateIfExists")
  Boolean updateIfExists;

  @Expose
  @SerializedName("Settings")
  Map<String, String> settings;

  @Expose
  @SerializedName("OdpsEndpoint")
  String odpsEndpoint;

  @Expose
  @SerializedName("OdpsAccessKeyId")
  String odpsAccessKeyId;

  @Expose
  @SerializedName("OdpsAccessKeySecret")
  String odpsAccessKeySecret;

  @Expose
  @SerializedName("OssEndpoint")
  String ossEndpoint;

  @Expose
  @SerializedName("OssLocalEndpoint")
  String ossLocalEndpoint;

  @Expose
  @SerializedName("OssBucket")
  String ossBucket;

  @Expose
  @SerializedName("OssRoleArn")
  String ossRoleArn;

  @Expose
  @SerializedName("OssAccessKeyId")
  String ossAccessKeyId;

  @Expose
  @SerializedName("OssAccessKeySecret")
  String ossAccessKeySecret;

  @Override
  public void validate() throws MmaException {
    if (!DataSource.ODPS.equals(this.dataSource)) {
      throw new MmaException("DataSource cannot be null or empty or any value other than ODPS");
    }

    if (StringUtils.isNullOrEmpty(this.sourceProjectName)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("SourceProjectName");
    }

    if (StringUtils.isNullOrEmpty(this.destProjectName)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("DestProjectName");
    }

    if (this.objectTypes == null || this.objectTypes.isEmpty()) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("ObjectTypes");
    }

    if (StringUtils.isNullOrEmpty(this.backupName)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("BackupName");
    }

    if (StringUtils.isNullOrEmpty(this.odpsEndpoint)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OdpsEndpoint");
    }

    if (StringUtils.isNullOrEmpty(this.odpsAccessKeyId)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OdpsAccessKeyId");
    }

    if (StringUtils.isNullOrEmpty(this.odpsAccessKeySecret)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OdpsAccessKeySecret");
    }

    if (StringUtils.isNullOrEmpty(this.ossEndpoint)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OssEndpoint");
    }

    if (StringUtils.isNullOrEmpty(this.ossLocalEndpoint)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OssLocalEndpoint");
    }

    if (StringUtils.isNullOrEmpty(this.ossBucket)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OssBucket");
    }

    if (StringUtils.isNullOrEmpty(this.ossAccessKeyId)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OssAccessKeyId");
    }

    if (StringUtils.isNullOrEmpty(this.ossAccessKeySecret)) {
      throw MmaExceptionFactory.getAttributeCannotBeNullOrEmptyException("OssAccessKeySecret");
    }

    ConfigUtils.validateOdps(
        this.odpsEndpoint, this.sourceProjectName, this.odpsAccessKeyId, this.odpsAccessKeySecret);
    ConfigUtils.validateOss(
        this.ossLocalEndpoint, this.ossBucket, this.ossAccessKeyId, this.ossAccessKeySecret);
  }

  public String getJobId() {
    return this.jobId;
  }

  public DataSource getDataSource() {
    if (this.dataSource == null) {
      return MmaServerConfig.getInstance().getDataSource();
    }

    return this.dataSource;
  }

  public String getSourceProjectName() {
    return this.sourceProjectName;
  }

  public String getDestProjectName() {
    return this.destProjectName;
  }

  public List<MmaConfig.ObjectType> getObjectTypes() {
    if (this.objectTypes != null) {
      return new LinkedList(this.objectTypes);
    }
    return null;
  }

  public String getBackupName() {
    return this.backupName;
  }

  public Boolean getUpdateIfExists() {
    return this.updateIfExists;
  }

  public Map<String, String> getSettings() {
    if (this.settings != null) {
      return new HashMap(this.settings);
    }
    return null;
  }

  public String getOdpsEndpoint() {
    if (this.odpsEndpoint == null) {
      MmaConfig.OdpsConfig odpsConfig = MmaServerConfig.getInstance().getOdpsConfig();
      return (odpsConfig == null) ? null : odpsConfig.getEndpoint();
    }
    return this.odpsEndpoint;
  }

  public String getOdpsAccessKeyId() {
    if (this.odpsAccessKeyId == null) {
      MmaConfig.OdpsConfig odpsConfig = MmaServerConfig.getInstance().getOdpsConfig();
      return (odpsConfig == null) ? null : odpsConfig.getAccessId();
    }
    return this.odpsAccessKeyId;
  }

  public String getOdpsAccessKeySecret() {
    if (this.odpsAccessKeySecret == null) {
      MmaConfig.OdpsConfig odpsConfig = MmaServerConfig.getInstance().getOdpsConfig();
      return (odpsConfig == null) ? null : odpsConfig.getAccessKey();
    }
    return this.odpsAccessKeySecret;
  }

  public String getOssEndpoint() {
    if (this.ossEndpoint == null) {
      MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
      return (ossConfig == null) ? null : ossConfig.getOssEndpoint();
    }
    return this.ossEndpoint;
  }

  public String getOssLocalEndpoint() {
    if (this.ossLocalEndpoint == null) {
      MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
      return (ossConfig == null) ? null : ossConfig.getOssLocalEndpoint();
    }
    return this.ossLocalEndpoint;
  }

  public String getOssBucket() {
    if (this.ossBucket == null) {
      MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
      return (ossConfig == null) ? null : ossConfig.getOssBucket();
    }
    return this.ossBucket;
  }

  public String getOssRoleArn() {
    if (this.ossRoleArn == null) {
      MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
      return (ossConfig == null) ? null : ossConfig.getOssRoleArn();
    }
    return this.ossRoleArn;
  }

  public String getOssAccessKeyId() {
    if (this.ossAccessKeyId == null) {
      MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
      return (ossConfig == null) ? null : ossConfig.getOssAccessId();
    }
    return this.ossAccessKeyId;
  }

  public String getOssAccessKeySecret() {
    if (this.ossAccessKeySecret == null) {
      MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
      return (ossConfig == null) ? null : ossConfig.getOssAccessKey();
    }
    return this.ossAccessKeySecret;
  }
}
