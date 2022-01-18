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
import com.aliyun.odps.mma.meta.model.FunctionMetaModel;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.meta.model.ResourceMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.util.GsonUtils;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;

import static com.aliyun.odps.mma.config.ObjectType.TABLE;
import static com.aliyun.odps.mma.config.ObjectType.RESOURCE;
import static com.aliyun.odps.mma.config.ObjectType.FUNCTION;

public class OssMetaSource implements MetaSource {

  private static final String DELIMITER = "/";
  private static final String DEFAULT_ROOT_PREFIX = "mma";
  private static final String META_FOLDER = "metadata";
  private static final String METAFILE = "meta.txt";
  private static final String DATA_FOLDER = "data";

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
    String dirPath = getPath(databaseName, null, null, null);
    return dirExists(dirPath);
  }

  @Override
  public boolean hasTable(String databaseName, String tableName) {
    return dirExists(getPath(databaseName, TABLE, tableName, null));
  }

  @Override
  public boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception {
    TableMetaModel tableMetaModel = getMetaModel(databaseName, tableName, TABLE, TableMetaModel.class);
    return tableMetaModel.getPartitions()
        .stream()
        .anyMatch(p -> p.getPartitionValues().equals(partitionValues));
  }

  @Override
  public List<String> listDatabases() {
    return listObjects(null, null);
  }

  @Override
  public List<String> listTables(String databaseName) {
    return listObjects(databaseName, TABLE);
  }

  @Override
  public List<String> listResources(String databaseName) throws Exception {
    return listObjects(databaseName, RESOURCE);
  }

  @Override
  public List<String> listFunctions(String databaseName) throws Exception {
    return listObjects(databaseName, FUNCTION);
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) throws Exception {
    TableMetaModel tableMetaModel = getMetaModel(databaseName, tableName, TABLE, TableMetaModel.class);
    return tableMetaModel.getPartitions()
        .stream()
        .map(PartitionMetaModel::getPartitionValues)
        .collect(Collectors.toList());
  }

  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    return getMetaModel(databaseName, tableName, TABLE, TableMetaModel.class);
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName, String tableName)
      throws Exception {
    return getMetaModel(databaseName, tableName, TABLE, TableMetaModel.class);
  }

  @Override
  public PartitionMetaModel getPartitionMeta(
      String databaseName,
      String tableName,
      List<String> partitionValues) throws Exception {
    TableMetaModel tableMetaModel = getMetaModel(databaseName, tableName, TABLE, TableMetaModel.class);
    return tableMetaModel.getPartitions()
        .stream()
        .filter(p -> partitionValues.equals(p.getPartitionValues()))
        .findFirst()
        .orElse(null);
  }

  @Override
  public ResourceMetaModel getResourceMeta(String databaseName, String resourceName)
      throws Exception {
    return getMetaModel(databaseName, resourceName, RESOURCE, ResourceMetaModel.class);
  }

  @Override
  public FunctionMetaModel getFunctionMeta(String databaseName, String functionName)
      throws Exception {
    return getMetaModel(databaseName, functionName, FUNCTION, FunctionMetaModel.class);
  }

  @Override
  public List<ObjectType> getSupportedObjectTypes() {
    return new ArrayList<>(SUPPORTED_OBJECT_TYPES);
  }

  @Override
  public void shutdown() {
    oss.shutdown();
  }

  private String getPath(String catalog, ObjectType objectType, String objectName, String file) {
    return getPath(ossPath, true, catalog, objectType, objectName, file);
  }

  public static String[] getMetaAndDataPath(String ossPathPrefix, String catalogName,
                                            ObjectType objectType, String objectName) {
    // get [metafile path, data dir]
    return new String[]{
        getPath(ossPathPrefix, true, catalogName, objectType, objectName, METAFILE),
        getPath(ossPathPrefix, false, catalogName, objectType, objectName, null)
    };
  }

  public static String getDefaultPrefix(String prefix, String rootJobId) {
    // prefix not null => prefix
    // prefix ==  null => mma/rootJobId
    if (StringUtils.isNullOrEmpty(prefix)) {
      return getFolderNameWithSeparator(DEFAULT_ROOT_PREFIX) + getFolderNameWithSeparator(
          rootJobId);
    }
    return prefix;
  }

  public static String getPath(String ossPathPrefix, boolean isMetaData, String catalogName,
                               ObjectType objectType, String objectName, String file) {
    // prefix / data(metadata) / catalog name / object type(eg: TABLE) / object name / (file)

    StringBuilder builder = new StringBuilder();
    String dataType = isMetaData ? META_FOLDER : DATA_FOLDER;
    builder.append(getFolderNameWithSeparator(ossPathPrefix))
        .append(getFolderNameWithSeparator(dataType));

    if (null == catalogName) {
      return builder.toString();
    }
    builder.append(getFolderNameWithSeparator(catalogName));

    if (null == objectType) {
      return builder.toString();
    }
    builder.append(getFolderNameWithSeparator(objectType.name()));

    if (null == objectName) {
      return builder.toString();
    }
    builder.append(getFolderNameWithSeparator(objectName));

    if (null == file) {
      return builder.toString();
    }
    builder.append(file);

    return builder.toString();
  }

  private static String getFolderNameWithSeparator(String folderName) {
    if (folderName.endsWith("/")) {
      return folderName;
    }
    return folderName + "/";
  }

  private List<String> listObjects(String databaseName, ObjectType objectType) {
    String path = getPath(databaseName, objectType, null, null);
    return listObjects(path);
  }

  private <T> T getMetaModel(String databaseName, String objectName, ObjectType type, Class<T> cls)
      throws IOException {
    String path = getPath(databaseName, type, objectName, METAFILE);
    return GsonUtils.GSON.fromJson(readObject(path), cls);
  }

  private String readObject(String path) throws IOException {
    OSSObject ossObject = oss.getObject(ossBucket, path);
    return IOUtils.toString(ossObject.getObjectContent(), StandardCharsets.UTF_8);
  }

  private boolean dirExists(String path) {
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(ossBucket);
    listObjectsRequest.setMaxKeys(1);
    listObjectsRequest.setPrefix(path);
    listObjectsRequest.setDelimiter(DELIMITER);
    return !oss.listObjects(listObjectsRequest).getCommonPrefixes().isEmpty();
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
