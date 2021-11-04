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

package com.aliyun.odps.mma.config;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.MapUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.utils.StringUtils;

public class OdpsConfig implements Configuration{

  private String accessId;
  private String accessKey;
  private String endpoint;
  private String projectName;
  private Map<String, String> globalSettings;
  private MmaConfig.SQLSettingConfig sourceTableSettings;
  private MmaConfig.SQLSettingConfig destinationTableSettings;

  public OdpsConfig(String accessId, String accessKey, String endpoint, String projectName) {
    this.accessId = accessId;
    this.accessKey = accessKey;
    this.endpoint = endpoint;
    this.projectName = projectName;
  }

  public String getAccessId() {
    return accessId;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getProjectName() {
    return projectName;
  }

  public Map<String, String> getGlobalSettings() {
    return globalSettings == null ? MapUtils.EMPTY_MAP : globalSettings;
  }

  public MmaConfig.SQLSettingConfig getSourceTableSettings() {
    if (sourceTableSettings == null) {
      sourceTableSettings = new MmaConfig.SQLSettingConfig();
    }
    if (!sourceTableSettings.isInitialized()) {
      sourceTableSettings.initialize(
          globalSettings == null ? MapUtils.EMPTY_MAP : globalSettings);
    }
    return sourceTableSettings;
  }

  public MmaConfig.SQLSettingConfig getDestinationTableSettings() {
    if (destinationTableSettings == null) {
      destinationTableSettings = new MmaConfig.SQLSettingConfig();
    }
    if (!destinationTableSettings.isInitialized()) {
      destinationTableSettings.initialize(
          globalSettings == null ? MapUtils.EMPTY_MAP : globalSettings);
    }
    return destinationTableSettings;
  }

  public Odps toOdps() {
    AliyunAccount aliyunAccount = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(aliyunAccount);
    odps.setDefaultProject(this.projectName);
    odps.setEndpoint(this.endpoint);

    return odps;
  }

  @Override
  public void validate() {
    //TODO
    // return (!StringUtils.isNullOrEmpty(accessId) &&
    //         !StringUtils.isNullOrEmpty(accessKey) &&
    //         !StringUtils.isNullOrEmpty(endpoint) &&
    //         !StringUtils.isNullOrEmpty(projectName));
  }

  @Override
  public String toString() {
    return "OdpsConfig {"
           + "accessId='" + Objects.toString(accessId, "null") + '\''
           + ", accessKey='" + Objects.toString(accessKey, "null") + '\''
           + ", endpoint='" + Objects.toString(endpoint, "null") + '\''
           + ", projectName='" + Objects.toString(projectName, "null") + '\''
           + ", globalSettings=" + Objects.toString(globalSettings, "null")
           + ", sourceTableSettings=" + Objects.toString(sourceTableSettings, "null")
           + ", destinationTableSettings=" + Objects.toString(destinationTableSettings, "null")
           + '}';
  }
}
