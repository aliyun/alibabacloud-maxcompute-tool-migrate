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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.Bucket;

public class ConfigUtils {
  public static void validateOdps(
      String endpoint,
      String project,
      String accessKeyId,
      String accessKeySecret) throws MmaException {

    AliyunAccount aliyunAccount = new AliyunAccount(accessKeyId, accessKeySecret);
    Odps odps = new Odps(aliyunAccount);
    odps.setEndpoint(endpoint);

    try {
      odps.projects().get(project).reload();
    } catch (OdpsException e) {
      throw new MmaException("Invalid ODPS configuration", e);
    }
  }

  public static void validateOss(
      String endpoint,
      String bucket,
      String accessKeyId,
      String accessKeySecret) throws MmaException {

    OSS oss = (new OSSClientBuilder()).build(endpoint, accessKeyId, accessKeySecret);
    if (oss.listBuckets().stream().noneMatch(b -> bucket.equals(b.getName()))) {
      throw new MmaException("Invalid OSS configuration");
    }
  }
}
