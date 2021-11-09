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

import com.aliyun.odps.mma.config.MmaConfig;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.PutObjectRequest;


public class OssUtils
{
  private static final Logger LOG = LogManager.getLogger(OssUtils.class);

  public static void createFile(MmaConfig.OssConfig ossConfig, String fileName, String content) {
    createFile(ossConfig, fileName, new ByteArrayInputStream(content.getBytes()));
  }

  public static void createFile(
      MmaConfig.OssConfig ossConfig,
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

  public static boolean exists(MmaConfig.OssConfig ossConfig, String fileName) {
    LOG.info("Check oss file: {}, endpoint: {}, bucket: {}", fileName, ossConfig
        .getEndpointForMma(), ossConfig.getOssBucket());

    OSS ossClient = createOssClient(ossConfig);
    boolean exists = ossClient.doesObjectExist(ossConfig.getOssBucket(), fileName);
    ossClient.shutdown();
    return exists;
  }

  public static String downloadFile(
      MmaConfig.OssConfig ossConfig,
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

  private static OSS createOssClient(MmaConfig.OssConfig ossConfig) {
    return (new OSSClientBuilder()).build(
        ossConfig.getEndpointForMma(),
        ossConfig.getOssAccessId(),
        ossConfig.getOssAccessKey());
  }

  private static String getFolderNameWithSeparator(String folderName) {
    if (folderName.endsWith("/")) {
      return folderName;
    }
    return folderName + "/";
  }
}
