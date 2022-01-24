/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.odps.mma.meta;

import java.util.Map;

public class HiveMetaConfig implements MetaConfig{

  private String metaStoreUri;
  private String principal;
  private String keytab;
  private Map<String, String> javaSecurityConfigs;
  private Map<String, String> extraConfigs;
  private boolean saslEnabled;
  private String jdbcUrl;
  private String jdbcUser;
  private String jdbcPwd;
  private boolean useHms;

  public HiveMetaConfig(String metaStoreUri,
                        boolean saslEnabled,
                        String principal,
                        String keytab,
                        Map<String, String> javaSecurityConfigs,
                        Map<String, String> extraConfigs) {
    this.useHms = true;
    this.metaStoreUri = metaStoreUri;
    this.saslEnabled = saslEnabled;
    this.principal = principal;
    this.keytab = keytab;
    this.javaSecurityConfigs = javaSecurityConfigs;
    this.extraConfigs = extraConfigs;
  }

  public HiveMetaConfig(String jdbcUrl, String jdbcUser, String jdbcPwd,
                        Map<String, String> javaSecurityConfigs) {
    this.useHms = false;
    this.jdbcUrl = jdbcUrl;
    this.jdbcUser = jdbcUser;
    this.jdbcPwd = jdbcPwd;
    this.javaSecurityConfigs = javaSecurityConfigs;
  }

  public String getMetaStoreUri() {
    return metaStoreUri;
  }

  public String getPrincipal() {
    return principal;
  }

  public String getKeytab() {
    return keytab;
  }

  public Map<String, String> getJavaSecurityConfigs() {
    return javaSecurityConfigs;
  }

  public Map<String, String> getExtraConfigs() {
    return extraConfigs;
  }

  public boolean isSaslEnabled() {
    return saslEnabled;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getJdbcUser() {
    return jdbcUser;
  }

  public String getJdbcPwd() {
    return jdbcPwd;
  }

  public boolean isUseHms() {
    return useHms;
  }
}
