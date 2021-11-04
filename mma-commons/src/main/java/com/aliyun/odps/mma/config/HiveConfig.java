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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.MapUtils;

import com.aliyun.odps.utils.StringUtils;

public class HiveConfig implements MmaConfig.Config {

  private String jdbcConnectionUrl;
  private String user;
  private String password;
  private String hmsThriftAddr;
  private String krbPrincipal;
  private String keyTab;
  private List<String> krbSystemProperties;
  private Map<String, String> globalSettings;
  private MmaConfig.SQLSettingConfig sourceTableSettings;

  public HiveConfig(
      String jdbcConnectionUrl,
      String user,
      String password,
      String hmsThriftAddr,
      String krbPrincipal,
      String keyTab,
      List<String> krbSystemProperties,
      Map<String, String> globalSettings,
      MmaConfig.SQLSettingConfig sourceTableSettings) {
    this.jdbcConnectionUrl = jdbcConnectionUrl;
    this.user = user;
    this.password = password;
    this.hmsThriftAddr = hmsThriftAddr;
    this.krbPrincipal = krbPrincipal;
    this.keyTab = keyTab;
    this.krbSystemProperties = krbSystemProperties;
    this.globalSettings = globalSettings;
    this.sourceTableSettings = sourceTableSettings;
  }

  @Override
  public boolean validate() {
    return (!StringUtils.isNullOrEmpty(jdbcConnectionUrl)
            && user != null
            && password != null);
  }

  public String getJdbcConnectionUrl() {
    return jdbcConnectionUrl;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getHmsThriftAddr() {
    return hmsThriftAddr;
  }

  public String getKrbPrincipal() {
    return krbPrincipal;
  }

  public String getKeyTab() {
    return keyTab;
  }

  public List<String> getKrbSystemProperties() {
    return krbSystemProperties;
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

  @Override
  public String toString() {
    return "HiveConfig {" + "hiveJdbcAddress='"
           + Objects.toString(jdbcConnectionUrl, "null") + '\''
           + ", hmsThriftAddr='"
           + Objects.toString(hmsThriftAddr, "null") + '\''
           + ", krbPrincipal='"
           + Objects.toString(krbPrincipal, "null") + '\''
           + ", keyTab='" + Objects.toString(keyTab, "null") + '\''
           + ", krbSystemProperties="
           + Objects.toString(krbSystemProperties, "null")
           + ", sourceTableSettings=" + Objects.toString(sourceTableSettings, "null")
           + '}';
  }
}
