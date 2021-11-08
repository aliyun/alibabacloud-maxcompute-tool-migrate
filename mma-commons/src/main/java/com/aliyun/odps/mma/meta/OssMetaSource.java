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

package com.aliyun.odps.mma.meta;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.util.GsonUtils;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;

public class OssMetaSource implements MetaSource {

  private static final String DELIMITER = "/";
  private static final List<ObjectType> SUPPORTED_OBJECT_TYPES;
  static {
    SUPPORTED_OBJECT_TYPES = new ArrayList<>(1);
    SUPPORTED_OBJECT_TYPES.add(ObjectType.TABLE);
    SUPPORTED_OBJECT_TYPES.add(ObjectType.RESOURCE);
    SUPPORTED_OBJECT_TYPES.add(ObjectType.FUNCTION);
  }

  private String ossBucket;
  private String ossPath;
  private OSS oss;

  public OssMetaSource(
      String ossAccessKeyId,
      String ossAccessKeySecret,
      String ossBucket,
      String ossPath,
      String ossEndpoint) {
    this.ossBucket = Objects.requireNonNull(ossBucket);
    if (ossPath.startsWith(DELIMITER)) {
      ossPath = ossPath.substring(1);
    }
    if (ossPath.endsWith(DELIMITER)) {
      ossPath = ossPath.substring(0, ossPath.length() - 1);
    }
    this.ossPath = ossPath;
    this.oss = new OSSClientBuilder().build(ossEndpoint, ossAccessKeyId, ossAccessKeySecret);
  }

  @Override
  public boolean hasDatabase(String databaseName) {
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(ossBucket);
    listObjectsRequest.setMaxKeys(1);
    listObjectsRequest.setPrefix(ossPath + "/metadata/" + databaseName);
    listObjectsRequest.setDelimiter(DELIMITER);
    return !oss.listObjects(listObjectsRequest).getCommonPrefixes().isEmpty();
  }

  @Override
  public boolean hasTable(String databaseName, String tableName) {
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(ossBucket);
    listObjectsRequest.setMaxKeys(1);
    listObjectsRequest.setPrefix(ossPath + "/metadata/" + databaseName + "/TABLE/" + tableName);
    listObjectsRequest.setDelimiter(DELIMITER);
    return !oss.listObjects(listObjectsRequest).getCommonPrefixes().isEmpty();
  }

  @Override
  public boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception {
    String objectPath = ossPath + "/metadata/" + databaseName + "/TABLE/" + tableName + "/meta.txt";
    TableMetaModel tableMetaModel =
        GsonUtils.GSON.fromJson(readObject(objectPath), TableMetaModel.class);
    return tableMetaModel.getPartitions()
        .stream()
        .anyMatch(p -> p.getPartitionValues().equals(partitionValues));
  }

  @Override
  public List<String> listDatabases() {
    String prefix = ossPath + "/metadata/";
    return listObjects(prefix);
  }

  @Override
  public List<String> listTables(String databaseName) {
    String prefix = ossPath + "/metadata/" + databaseName + "/TABLE/";
    return listObjects(prefix);
  }

  @Override
  public List<String> listResources(String databaseName) throws Exception {
    String prefix = ossPath + "/metadata/" + databaseName + "/RESOURCE/";
    return listObjects(prefix);
  }

  @Override
  public List<String> listFunctions(String databaseName) throws Exception {
    String prefix = ossPath + "/metadata/" + databaseName + "/FUNCTION/";
    return listObjects(prefix);
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) throws Exception {
    String objectPath = ossPath + "/metadata/" + databaseName + "/TABLE/" + tableName + "/meta.txt";
    TableMetaModel tableMetaModel =
        GsonUtils.GSON.fromJson(readObject(objectPath), TableMetaModel.class);
    return tableMetaModel.getPartitions()
        .stream()
        .map(PartitionMetaModel::getPartitionValues)
        .collect(Collectors.toList());
  }

  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    String objectPath = ossPath + "/metadata/" + databaseName + "/TABLE/" + tableName + "/meta.txt";
    return GsonUtils.GSON.fromJson(readObject(objectPath), TableMetaModel.class);
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName, String tableName)
      throws Exception {
    String objectPath = ossPath + "/metadata/" + databaseName + "/TABLE/" + tableName + "/meta.txt";
    return GsonUtils.GSON.fromJson(readObject(objectPath), TableMetaModel.class);
  }

  @Override
  public PartitionMetaModel getPartitionMeta(
      String databaseName,
      String tableName,
      List<String> partitionValues) throws Exception {
    String objectPath = ossPath + "/metadata/" + databaseName + "/TABLE/" + tableName + "/meta.txt";
    TableMetaModel tableMetaModel =
        GsonUtils.GSON.fromJson(readObject(objectPath), TableMetaModel.class);
    return tableMetaModel.getPartitions()
        .stream()
        .filter(p -> partitionValues.equals(p.getPartitionValues()))
        .findFirst()
        .orElse(null);
  }

  @Override
  public ResourceMetaModel getResourceMeta(String databaseName, String resourceName)
      throws Exception {
    throw new MmaException("get oss resource not supported");
  }

  @Override
  public FunctionMetaModel getFunctionMeta(String databaseName, String functionName)
      throws Exception {
    String objectPath = ossPath + "/metadata/" + databaseName + "/TABLE/" + functionName + "/meta.txt";
    FunctionMetaModel functionMetaModel = GsonUtils.GSON.fromJson(readObject(objectPath), FunctionMetaModel.class);
    return functionMetaModel;
  }

  @Override
  public List<ObjectType> getSupportedObjectTypes() {
    return new ArrayList<>(SUPPORTED_OBJECT_TYPES);
  }

  @Override
  public void shutdown() {
    oss.shutdown();
  }

  private String readObject(String path) throws IOException {
    OSSObject ossObject = oss.getObject(ossBucket, path);
    return IOUtils.toString(ossObject.getObjectContent(), StandardCharsets.UTF_8);
  }

  private List<String> listObjects(String prefix) {
    List<String> ret = new LinkedList<>();

    ObjectListing objectListing;
    String nextMarker = null;
    do {
      objectListing = oss.listObjects(
          new ListObjectsRequest(ossBucket)
              .withDelimiter(DELIMITER)
              .withMarker(nextMarker)
              .withMaxKeys(100)
              .withPrefix(prefix));
      nextMarker = objectListing.getNextMarker();

      List<String> entries = objectListing
          .getCommonPrefixes()
          .stream()
          .map(o -> {
            String[] splits = o.split(DELIMITER);
            return splits[splits.length - 1];
          })
          .collect(Collectors.toList());
      ret.addAll(entries);
    } while (objectListing.isTruncated());

    return ret;
  }
}
