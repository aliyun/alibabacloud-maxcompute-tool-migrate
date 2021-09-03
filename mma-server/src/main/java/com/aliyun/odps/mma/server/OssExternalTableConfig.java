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

import com.aliyun.odps.mma.config.ExternalTableConfig;
import com.aliyun.odps.mma.config.ExternalTableStorage;

public class OssExternalTableConfig extends ExternalTableConfig {
  private String endpoint;
  private String bucket;
  private String roleRan;

  public OssExternalTableConfig(String endpoint, String bucket, String roleRan, String location) {
    super(ExternalTableStorage.OSS, location);
    this.endpoint = endpoint;
    this.bucket = bucket;
    this.roleRan = roleRan;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getBucket() {
    return bucket;
  }

  public String getRoleRan() {
    return roleRan;
  }
}
