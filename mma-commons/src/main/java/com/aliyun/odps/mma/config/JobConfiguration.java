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

package com.aliyun.odps.mma.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class JobConfiguration extends AbstractConfiguration {

  /**
   * Object type. Could be TABLE, VIRTUAL_VIEW, PARTITION, RESOURCE, FUNCTION.
   */
  public static final String OBJECT_TYPE = "mma.object.type";

  /**
   * Source object position.
   */
  public static final String SOURCE_CATALOG_NAME = "mma.object.source.catalog.name";
  public static final String SOURCE_OBJECT_TYPES = "mma.object.source.types";
  //  public static final String SOURCE_SCHEMA_NAME = "mma.object.source.schema.name";
  public static final String SOURCE_OBJECT_NAME = "mma.object.source.name";
  public static final String PARTITION_BEGIN = "mma.filter.partition.begin";
  public static final String PARTITION_END = "mma.filter.partition.end";
  public static final String PARTITION_ORDER = "mma.filter.partition.orders";

  /**
   * Source Object attributes.
   */
  // Last modified time of the source object when the job is created or reset. Can be used to
  // determine whether the source object has changed. The job configuration would not contain this
  // key when the last modified time is not valid.
  public static final String SOURCE_OBJECT_LAST_MODIFIED_TIME = "mma.object.mtime";

  /**
   * Dest object position.
   */
  public static final String DEST_CATALOG_NAME = "mma.object.dest.catalog.name";
  //  public static final String DEST_SCHEMA_NAME = "mma.object.dest.schema.name";
  public static final String DEST_OBJECT_NAME = "mma.object.dest.name";


  /**
   * Job attributes.
   */
  public static final String JOB_ID = "mma.job.id";

  /**
   * Job dag task type, could be TableSetup/DataTrans/DataConsistent, split with comma.
   * @see {@link DagTaskType}
   */
  public static final String JOB_DAG_TASK_TYPES = "mma.job.dag.task.types";

  /**
   * Regex patterns
   */
  private static final Pattern JOB_ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");
  private static final String SPLIT_PATTERN = "\\s*/\\s*";

  public JobConfiguration(Map<String, String> configuration) {
    super(configuration);
  }

  public static JobConfiguration fromJson(String json) {
    Map<String, String> map =
        GsonUtils.GSON.fromJson(json, new TypeToken<Map<String, String>>() {
        }.getType());
    return new JobConfiguration(map);
  }

  @Override
  public void validate() throws MmaException {
    // Supported job id: [A-Za-z0-9-_]
    // Supported source and destination combinations:
    // 1. Hive(metadata), Hive(data) -> MC(metadata), MC(data)
    // 2. MC(metadata), MC(metadata) -> OSS(metadata), OSS(data)
    // 3. OSS(metadata), OSS(data) -> MC(metadata), MC(data)
    validateJobId();
    validPartitionFilter();
    MetaSourceType metaSourceType = MetaSourceType.valueOf(configuration.get(METADATA_SOURCE_TYPE));
    DataSourceType dataSourceType = DataSourceType.valueOf(configuration.get(DATA_SOURCE_TYPE));
    MetaDestType metaDestType = MetaDestType.valueOf(configuration.get(METADATA_DEST_TYPE));
    DataDestType dataDestType = DataDestType.valueOf(configuration.get(DATA_DEST_TYPE));
    if (metaSourceType.equals(MetaSourceType.MaxCompute)
        && dataSourceType.equals(DataSourceType.MaxCompute)
        && metaDestType.equals(MetaDestType.OSS)
        && dataDestType.equals(DataDestType.OSS)) {
      validateMcToOssCredentials();
    } else if (metaSourceType.equals(MetaSourceType.OSS)
               && dataSourceType.equals(DataSourceType.OSS)
               && metaDestType.equals(MetaDestType.MaxCompute)
               && dataDestType.equals(DataDestType.MaxCompute)) {
      validateOssToMcCredentials();
    } else if (metaSourceType.equals(MetaSourceType.Hive)
               && dataSourceType.equals(DataSourceType.Hive)
               && metaDestType.equals(MetaDestType.MaxCompute)
               && dataDestType.equals(DataDestType.MaxCompute)) {
      validateHiveToMcCredentials();
      validMcAuthType();
      validateDagTaskTypes();
    } else {
      throw new IllegalArgumentException("Unsupported source and dest combination.");
    }
  }

  public List<String> getPartitionBegin() {
    if (!containsKey(PARTITION_BEGIN)) {
      return new ArrayList<>();
    }
    return Arrays.asList(get(PARTITION_BEGIN).split(SPLIT_PATTERN));
  }

  public List<String> getPartitionEnd() {
    if (!containsKey(PARTITION_END)) {
      return new ArrayList<>();
    }
    return Arrays.asList(get(PARTITION_END).split(SPLIT_PATTERN));
  }

  public List<PartitionOrderType> getPartitionOrderType() {
    if (!containsKey(PARTITION_ORDER)) {
      return new ArrayList<>();
    }
    return Arrays
        .stream(get(PARTITION_ORDER).split(SPLIT_PATTERN))
        .map(PartitionOrderType::valueOf)
        .collect(Collectors.toList());
  }

  private void validateMcToOssCredentials() throws MmaException {
    ConfigurationUtils.validateMcMetaSource(this);
    ConfigurationUtils.validateMcDataSource(this);
    ConfigurationUtils.validateOssMetaDest(this);
    ConfigurationUtils.validateOssDataDest(this);
  }

  private void validateOssToMcCredentials() throws MmaException {
    ConfigurationUtils.validateOssMetaSource(this);
    ConfigurationUtils.validateOssDataSource(this);
    ConfigurationUtils.validateMcMetaDest(this);
    ConfigurationUtils.validateMcDataDest(this);
  }

  private void validateHiveToMcCredentials() throws MmaException {
    ConfigurationUtils.validateHiveMetaSource(this);
    ConfigurationUtils.validateHiveDataSource(this);
    ConfigurationUtils.validateMcMetaDest(this);
    ConfigurationUtils.validateMcDataDest(this);
  }

  private void validateJobId() throws MmaException {
    String jobId = get(JobConfiguration.JOB_ID);
    if (!StringUtils.isBlank(jobId) && !JOB_ID_PATTERN.matcher(jobId).matches()) {
      throw new MmaException("Invalid job Id. Job id pattern: [A-Za-z0-9_-]+");
    }
  }

  private void validPartitionFilter() throws MmaException {
    List<String> begin = getPartitionBegin();
    List<String> end = getPartitionEnd();
    List<PartitionOrderType> orderType = getPartitionOrderType();

    int orderTypeSize = orderType.size();
    int minTypeSize = Math.max(begin.size(), end.size());
    if (orderTypeSize < minTypeSize) {
      throw new MmaException(
          String.format("ERROR: partitionOrderType not enough, current size: %d, need >= %d",
                        orderTypeSize, minTypeSize));
    }

    Comparator<List<String>> cmp =
        new ConfigurationUtils.PartitionComparator(orderType);
    if (cmp.compare(begin, end) > 0) {
      throw new MmaException(String.format("ERROR: partition end %s < begin %s", begin, end));
    }
  }

  private void validMcAuthType() throws MmaException {
    // match any McAuthType
    // && if McAuthType == AK => CONFIG_PATH != null
    String authStr = getOrDefault(DATA_DEST_MC_AUTH_TYPE, DATA_DEST_MC_AUTH_TYPE_DEFAULT);
    boolean match = Arrays.stream(McAuthType.values()).anyMatch(t->t.name().equals(authStr));
    if (!match) {
      throw new MmaException("ERROR: Unsupported MC authType: " + authStr);
    }

    McAuthType authType = McAuthType.valueOf(authStr);
    if (McAuthType.AK.equals(authType)) {
      if (StringUtils.isBlank(get(DATA_DEST_MC_CONFIG_PATH))) {
        throw new MmaException("ERROR: odps_config.ini path not set");
      }
    }
  }

  private void validateDagTaskTypes() throws MmaException {
    String taskTypesStr = get(JOB_DAG_TASK_TYPES);
    if (!StringUtils.isBlank(taskTypesStr)) {
      String[] taskTypes = taskTypesStr.split(",");
      for (String taskType : taskTypes) {
        try {
          DagTaskType type = DagTaskType.valueOf(taskType.trim());
        } catch (Throwable t) {
          throw new MmaException("ERROR: invalid " + JOB_DAG_TASK_TYPES);
        }
      }
    }
  }
}
