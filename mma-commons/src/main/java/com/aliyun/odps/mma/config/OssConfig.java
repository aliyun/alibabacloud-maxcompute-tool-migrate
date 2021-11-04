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

import java.util.Objects;

import com.aliyun.odps.utils.StringUtils;

public class OssConfig implements MmaConfig.Config {

  private String ossEndPointForMc;
  private String ossEndPointForMma;
  private String ossBucket;
  private String ossRoleArn;
  private String ossAccessId;
  private String ossAccessKey;
  private String metadataPath;
  private String dataPath;
  public static final String PREFIX = "oss://";

  public OssConfig(String internalEndpoint, String externalEndpoint, String ossBucket,
                   String roleArn, String accessId, String accessKey) {
    this.ossEndPointForMc = internalEndpoint;
    this.ossEndPointForMma = externalEndpoint == null ? internalEndpoint : externalEndpoint;
    this.ossBucket = ossBucket;
    this.ossRoleArn = roleArn;
    this.ossAccessId = accessId;
    this.ossAccessKey = accessKey;
  }

  @Override
  public boolean validate() {
    // TODO: try to connect
    if (StringUtils.isNullOrEmpty(ossEndPointForMc) ||
        StringUtils.isNullOrEmpty(ossEndPointForMma) ||
        StringUtils.isNullOrEmpty(ossBucket)) {
      return false;
    }
    // arn, accessId and accessKey should not be empty at the same time
    if (StringUtils.isNullOrEmpty(ossRoleArn) &&
        StringUtils.isNullOrEmpty(ossAccessId) &&
        StringUtils.isNullOrEmpty(ossAccessKey)) {
      return false;
    }
    if (StringUtils.isNullOrEmpty(ossAccessId) != StringUtils.isNullOrEmpty(ossAccessKey)) {
      return false;
    }
    return true;
  }

  public String getEndpointForMc() {
    return ossEndPointForMc;
  }

  public String getEndpointForMma() {
    return ossEndPointForMma;
  }

  public String getOssBucket() {
    return ossBucket;
  }

  public String getOssRoleArn() {
    return ossRoleArn;
  }

  public String getOssAccessId() {
    return ossAccessId;
  }

  public String getOssAccessKey() {
    return ossAccessKey;
  }

  @Override
  public String toString() {
    return "OssDataSource {"
           + "ossEndpoint='" + Objects.toString(ossEndPointForMc, "null") + '\''
           + ", ossLocalEndpoint='" + Objects.toString(ossEndPointForMma, "null") + '\''
           + ", ossBucket='" + Objects.toString(ossBucket, "null") + '\''
           + ", ossRoleArn='" + Objects.toString(ossRoleArn, "null") + '\''
           + '}';
  }
}
