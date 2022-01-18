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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.OssConfig;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.PutObjectRequest;


public class OssUtils
{
  private static final Logger LOG = LogManager.getLogger(OssUtils.class);

  public static void createFile(OssConfig ossConfig, String fileName, String content) {
    createFile(ossConfig, fileName, new ByteArrayInputStream(content.getBytes()));
  }

  public static void createFile(
      OssConfig ossConfig,
      String fileName,
      InputStream inputStream) {
    LOG.info("Create oss file: {}, endpoint: {}, bucket: {}", fileName, ossConfig
        .getEndpointForMma(), ossConfig.getOssBucket());

    OSS ossClient = createOssClient(ossConfig);

    PutObjectRequest putObjectRequest =
        new PutObjectRequest(ossConfig.getOssBucket(), fileName, inputStream);
    ossClient.putObject(putObjectRequest);
    ossClient.shutdown();
  }

  public static boolean exists(OssConfig ossConfig, String fileName) {
    LOG.info("Check oss file: {}, endpoint: {}, bucket: {}", fileName, ossConfig
        .getEndpointForMma(), ossConfig.getOssBucket());

    OSS ossClient = createOssClient(ossConfig);
    boolean exists = ossClient.doesObjectExist(ossConfig.getOssBucket(), fileName);
    ossClient.shutdown();
    return exists;
  }

  public static String downloadFile(
      OssConfig ossConfig,
      String jobId,
      String fileName) throws IOException {

    LOG.info("Download oss file: {}, endpoint: {}, bucket: {}", fileName, ossConfig
        .getEndpointForMma(), ossConfig.getOssBucket());

    OSS ossClient = createOssClient(ossConfig);

    String mmaHome = System.getenv("MMA_HOME");
    Path localFilePath = Paths.get(mmaHome, "tmp", "oss", jobId, fileName);
    File localFile = localFilePath.toFile();
    if (!localFile.getParentFile().exists() && !localFile.getParentFile().mkdirs()) {
      String errorMsg = "Download oss file failed when creating local file: "
          + localFilePath.toAbsolutePath().toString();
      throw new IOException(errorMsg);
    }
    ossClient.getObject(new GetObjectRequest(ossConfig.getOssBucket(), fileName), localFile);
    ossClient.shutdown();
    LOG.info("Download oss file {} to local {}", fileName, localFile.getAbsolutePath());
    return localFile.getAbsolutePath();
  }

  public static OssConfig getOssConfig(
      JobConfiguration config, boolean isMeta, boolean isSource, String rootJobId)
      throws MmaException {
    if (!isMeta) {
      throw new MmaException("not supported now");
    }
    if (isSource) {
      return new OssConfig(
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_INTERNAL),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ENDPOINT_EXTERNAL),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_BUCKET),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ROLE_ARN),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_ID),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_ACCESS_KEY_SECRET),
          config.get(AbstractConfiguration.METADATA_SOURCE_OSS_PATH),
          rootJobId);
    } else {
      return new OssConfig(
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ENDPOINT_INTERNAL),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ENDPOINT_EXTERNAL),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_BUCKET),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ROLE_ARN),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ACCESS_KEY_ID),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_ACCESS_KEY_SECRET),
          config.get(AbstractConfiguration.METADATA_DEST_OSS_PATH),
          rootJobId);
    }
  }

  private static OSS createOssClient(OssConfig ossConfig) {
    return (new OSSClientBuilder()).build(
        ossConfig.getEndpointForMma(),
        ossConfig.getOssAccessId(),
        ossConfig.getOssAccessKey());
  }

}
