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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.Constants;
import com.aliyun.odps.mma.config.MmaConfig;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
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

  public static String readFile(MmaConfig.OssConfig ossConfig, String fileName) throws IOException {
    LOG.info("Read oss file: {}, endpoint: {}, bucket: {}", fileName, ossConfig
        .getEndpointForMma(), ossConfig.getOssBucket());

    OSS ossClient = createOssClient(ossConfig);
    OSSObject ossObject = ossClient.getObject(ossConfig.getOssBucket(), fileName);
    StringBuilder builder = new StringBuilder();
    BufferedReader reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()));
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      builder.append(line).append('\n');
    }
    reader.close();
    ossClient.shutdown();
    return builder.toString();
  }

  public static InputStream openInputStream(MmaConfig.OssConfig ossConfig, String fileName) {
    LOG.info("Read oss file: {}, endpoint: {}, bucket: {}", fileName, ossConfig
        .getEndpointForMma(), ossConfig.getOssBucket());

    OSS ossClient = createOssClient(ossConfig);
    OSSObject ossObject = ossClient.getObject(ossConfig.getOssBucket(), fileName);
    return ossObject.getObjectContent();
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

  public static List<String> listBucket(MmaConfig.OssConfig ossConfig, String prefix) {
    OSS ossClient = createOssClient(ossConfig);
    ArrayList<String> allFiles = new ArrayList<>();
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(ossConfig.getOssBucket());
    listObjectsRequest.setMaxKeys(1000);
    listObjectsRequest.setPrefix(prefix);
    listObjectsRequest.setDelimiter("/");
    while (true) {
      ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
      List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
      for (OSSObjectSummary s : sums) {
        allFiles.add(s.getKey());
      }
      if (!objectListing.isTruncated()) {
        break;
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    }
    return allFiles;
  }

  public static List<String> listBucket(
      MmaConfig.OssConfig ossConfig,
      String prefix,
      String delimiter) {

    OSS ossClient = createOssClient(ossConfig);
    ArrayList<String> allFiles = new ArrayList<>();
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(ossConfig.getOssBucket());
    listObjectsRequest.setMaxKeys(1000);
    listObjectsRequest.setPrefix(prefix);
    listObjectsRequest.setDelimiter(delimiter);
    while (true) {
      ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
      for (String fullName : objectListing.getCommonPrefixes()) {
        String[] splitResult = fullName.split(delimiter);
        allFiles.add(splitResult[splitResult.length - 1]);
      }
      if (!objectListing.isTruncated()) {
        break;
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    }
    return allFiles;
  }

  public static String getOssPathToExportObject(
      String taskName,
      String objectType,
      String database,
      String objectName,
      String ossFileName) {
    StringBuilder builder = new StringBuilder();
    builder.append(getFolderNameWithSeparator("mma"))
           .append(getFolderNameWithSeparator(taskName))
           .append(getFolderNameWithSeparator(objectType))
           .append(getFolderNameWithSeparator(database.toLowerCase()))
           .append(getFolderNameWithSeparator(objectName.toLowerCase()))
           .append(ossFileName);
    return builder.toString();
  }


  public static String[] getOssPaths(
      String ossPathPrefix,
      String rootJobId,
      String objectType,
      String catalogName,
      String objectName) {
    return new String[]{
            getMcToOssPath(ossPathPrefix, rootJobId, objectType, catalogName, objectName, true),
            getMcToOssPath(ossPathPrefix, rootJobId, objectType, catalogName, objectName, false)};
  }

  private static String getMcToOssPath(
      String ossPathPrefix,
      String rootJobId,
      String objectType,
      String catalogName,
      String objectName,
      boolean isMetadata) {
    // prefix / data(metadata) / catalog name / object type(eg: TABLE) / object name / (file)
    StringBuilder builder = new StringBuilder();
    if (StringUtils.isNullOrEmpty(ossPathPrefix)) {
      ossPathPrefix = getFolderNameWithSeparator("mma") + getFolderNameWithSeparator(rootJobId);
    }
    String dataType = isMetadata ? "metadata" : "data";
    String filename = isMetadata ? Constants.EXPORT_META_FILE_NAME : "";

    builder.append(getFolderNameWithSeparator(ossPathPrefix))
           .append(getFolderNameWithSeparator(dataType))
           .append(getFolderNameWithSeparator(catalogName))
           .append(getFolderNameWithSeparator(objectType))
           .append(getFolderNameWithSeparator(objectName))
           .append(filename);

    return builder.toString();
  }

  public static String getOssFolderToExportObject(
      String taskName,
      String folderName,
      String database) {

    StringBuilder builder = new StringBuilder();
    builder.append(getFolderNameWithSeparator("odps_mma/export_objects/"))
           .append(getFolderNameWithSeparator(taskName))
           .append(getFolderNameWithSeparator(folderName))
           .append(getFolderNameWithSeparator(database.toLowerCase()));
    return builder.toString();
  }

  public static void getTableModelLogInfo(TableMetaModel mcModel,
                                            TableMetaModel ossModel,
                                            TableMetaModel externalModel) {
    LOG.info("MC model location: {}, database: {}, table: {}",
             mcModel.getLocation(), mcModel.getDatabase(), mcModel.getTable());
    LOG.info("OSS model location: {}, database: {}, table: {}",
             ossModel.getLocation(), ossModel.getDatabase(), ossModel.getTable());
    LOG.info("MC model location: {}, database: {}, table: {}",
             externalModel.getLocation(), externalModel.getDatabase(), externalModel.getTable());
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
