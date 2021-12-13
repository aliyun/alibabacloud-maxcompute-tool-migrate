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

package com.aliyun.odps.mma.server.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.DataDestType;
import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MetaDestType;
import com.aliyun.odps.mma.config.MetaSourceType;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.job.utils.JobUtils;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSourceFactory;

public class JobManager {

  private static final Logger LOG = LogManager.getLogger(JobManager.class);

  private Map<String, Job> jobs = new ConcurrentHashMap<>();

  private MetaManager metaManager;
  private MetaSourceFactory metaSourceFactory;

  public JobManager(MetaManager metaManager, MetaSourceFactory metaSourceFactory) {
    this.metaManager = Objects.requireNonNull(metaManager);
    this.metaSourceFactory = Objects.requireNonNull(metaSourceFactory);
  }

  /**
   * Check the status of each job. Set the status of running jobs back to pending.
   */
  public synchronized void recover() throws Exception {
    LOG.info("Enter recover");
    List<Job> runningJobs = listJobByStatusInternal(JobStatus.RUNNING);
    for (Job job : runningJobs) {
      LOG.info("Recover job, job id: {}", job.getId());
      job.stop();
      job.reset(false);
    }
    LOG.info("Leave recover");
  }

  public synchronized String addJob(JobConfiguration config) throws Exception {
    String jobId = config.get(JobConfiguration.JOB_ID);
    if (jobId != null && metaManager.getJobById(jobId) != null) {
      throw new IllegalArgumentException("Job already exists: " + jobId);
    }

    LOG.info("Add job, job id: {}", jobId);

    MetaSourceType metaSourceType = MetaSourceType.valueOf(
        config.get(JobConfiguration.METADATA_SOURCE_TYPE));
    DataSourceType dataSourceType = DataSourceType.valueOf(
        config.get(JobConfiguration.DATA_SOURCE_TYPE));
    MetaDestType metaDestType = MetaDestType.valueOf(
        config.get(JobConfiguration.METADATA_DEST_TYPE));
    DataDestType dataDestType = DataDestType.valueOf(
        config.get(JobConfiguration.DATA_DEST_TYPE));

    if (metaSourceType.equals(MetaSourceType.MaxCompute)
        && dataSourceType.equals(DataSourceType.MaxCompute)
        && metaDestType.equals(MetaDestType.OSS)
        && dataDestType.equals(DataDestType.OSS)) {
      return addMcToOssJob(config);
    } else if (metaSourceType.equals(MetaSourceType.OSS)
               && dataSourceType.equals(DataSourceType.OSS)
               && metaDestType.equals(MetaDestType.MaxCompute)
               && dataDestType.equals(DataDestType.MaxCompute)) {
      return addOssToMcJob(config);
    } else if (metaSourceType.equals(MetaSourceType.Hive)
               && dataSourceType.equals(DataSourceType.Hive)
               && metaDestType.equals(MetaDestType.MaxCompute)
               && dataDestType.equals(DataDestType.MaxCompute)) {
      return addHiveToMcJob(config);
    } else {
      throw new IllegalArgumentException("Unsupported source and dest combination.");
    }
  }

  private String addHiveToMcJob(JobConfiguration config) throws Exception {
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG: {
        return addCatalogJob(config);
      }
      case TABLE: {
        return addTableJob(null, config);
      }
      default:
        throw new IllegalArgumentException("Unsupported object type " + objectType);
    }
  }

  private String addMcToOssJob(JobConfiguration config) throws Exception {
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG: {
        return addCatalogJob(config);
      }
      case TABLE: {
        return addTableJob(null, config);
      }
      case RESOURCE:
      case FUNCTION: {
        return addSimpleTransmissionJob(null, config);
      }
      default:
        throw new IllegalArgumentException("Unsupported object type " + objectType);
    }
  }

  private String addCatalogJob(JobConfiguration config) throws Exception {
    String jobId = config.get(JobConfiguration.JOB_ID);
    if (StringUtils.isBlank(jobId)) {
      jobId = JobUtils.generateJobId(false);
    }

    List<ObjectType> objectTypes;
    // Supported object types
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    List<ObjectType> supportedObjectTypes = metaSource.getSupportedObjectTypes();
    // Specified object types
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String[] objectTypeList;
    // If object types are not specified, use the supported object types
    if (config.containsKey(JobConfiguration.SOURCE_OBJECT_TYPES)) {
      objectTypeList = config.get(JobConfiguration.SOURCE_OBJECT_TYPES).split(",");
      objectTypes =
          Arrays.stream(objectTypeList).map(ObjectType::valueOf).collect(Collectors.toList());
      for (ObjectType objectType : objectTypes) {
        if (!supportedObjectTypes.contains(objectType)) {
          throw new IllegalArgumentException("Unsupported object type: " + objectType);
        }
      }
    } else {
      objectTypes = supportedObjectTypes;
    }

    for (ObjectType objectType : objectTypes) {
      List<String> objNames;
      switch (objectType) {
        case TABLE:
          objNames = metaSource.listTables(catalogName);
          break;
        case RESOURCE:
          objNames = metaSource.listResources(catalogName);
          break;
        case FUNCTION:
          objNames = metaSource.listFunctions(catalogName);
          break;
        default:
          throw new IllegalArgumentException("Unsupported object type " + objectType);
      }
      for (String objName : objNames) {
        LOG.info("add {} job, object name: {}", objectType, objName);
        Map<String, String> subConfig = new HashMap<>(config);
        subConfig.put(JobConfiguration.SOURCE_OBJECT_NAME, objName);
        subConfig.put(JobConfiguration.DEST_OBJECT_NAME, objName);
        subConfig.put(JobConfiguration.OBJECT_TYPE, objectType.name());
        if (ObjectType.TABLE.equals(objectType)) {
          addTableJob(jobId, catalogName, objName, new JobConfiguration(subConfig));
        } else {
          addSimpleTransmissionJob(jobId, new JobConfiguration(subConfig));
        }
      }
    }

    String jobPriorityString = config.getOrDefault(
        JobConfiguration.JOB_PRIORITY,
        JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE);
    int jobPriority = Integer.valueOf(jobPriorityString);
    String jobMaxAttemptTimesString = config.getOrDefault(
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES,
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES_DEFAULT_VALUE);
    int jobMaxAttemptTimes = Integer.valueOf(jobMaxAttemptTimesString);
    metaManager.addJob(
        jobId,
        jobPriority,
        jobMaxAttemptTimes,
        config.toString(),
        true);

    return jobId;
  }

  private String addTableJob(
      String parentJobId,
      JobConfiguration config) throws Exception {

    return addTableJob(
        parentJobId,
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME),
        config);
  }

  private String addSimpleTransmissionJob(
      String parentJobId,
      JobConfiguration config) {
    boolean isSubJob = !StringUtils.isBlank(parentJobId);
    String jobId;
    if (!isSubJob && config.containsKey(JobConfiguration.JOB_ID)) {
      jobId = config.get(JobConfiguration.JOB_ID);
    } else {
      jobId = JobUtils.generateJobId(isSubJob);
    }

    if (StringUtils.isBlank(jobId)) {
      jobId = JobUtils.generateJobId(false);
    }

    String jobPriorityString = config.getOrDefault(
        JobConfiguration.JOB_PRIORITY,
        JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE);
    int jobPriority = Integer.valueOf(jobPriorityString);
    String jobMaxAttemptTimesString = config.getOrDefault(
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES,
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES_DEFAULT_VALUE);
    int jobMaxAttemptTimes = Integer.valueOf(jobMaxAttemptTimesString);

    if (isSubJob) {
      metaManager.addSubJob(
          parentJobId,
          jobId,
          jobPriority,
          jobMaxAttemptTimes,
          config.toString(),
          false);
    } else {
      metaManager.addJob(
          jobId,
          jobPriority,
          jobMaxAttemptTimes,
          config.toString(),
          false);
    }
    return jobId;
  }

  private String addOssToMcJob(JobConfiguration config) throws Exception {
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG: {
        return addCatalogJob(config);
      }
      case TABLE: {
        return addTableJob(null, config);
      }
      case RESOURCE:
      case FUNCTION: {
        return addSimpleTransmissionJob(null, config);
      }
      default:
        throw new IllegalArgumentException("Unsupported object type " + objectType);
    }
  }

  private String addTableJob(
      String parentJobId,
      String catalogName,
      String tableName,
      JobConfiguration config) throws Exception {
    boolean isSubJob = !StringUtils.isBlank(parentJobId);
    String jobId;
    if (!isSubJob && config.containsKey(JobConfiguration.JOB_ID)) {
      jobId = config.get(JobConfiguration.JOB_ID);
    } else {
      jobId = JobUtils.generateJobId(isSubJob);
    }

    // Ignore schema name for now, since MetaSource interface doesn't support it.
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    TableMetaModel tableMetaModel = metaSource.getTableMeta(catalogName, tableName);

    String jobPriorityString = config.getOrDefault(
        JobConfiguration.JOB_PRIORITY,
        JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE);
    int jobPriority = Integer.valueOf(jobPriorityString);
    String jobMaxAttemptTimesString = config.getOrDefault(
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES,
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES_DEFAULT_VALUE);
    int jobMaxAttemptTimes = Integer.valueOf(jobMaxAttemptTimesString);

    boolean isPartitioned = !tableMetaModel.getPartitionColumns().isEmpty();

    Map<String, String> extraConfig = new HashMap<>();
    if (isPartitioned) {
      // Add each partition as a sub job
      JobUtils.PartitionFilter partitionFilter = new JobUtils.PartitionFilter(config);
      for (PartitionMetaModel partitionMetaModel : tableMetaModel.getPartitions()) {

        if (!partitionFilter.filter(partitionMetaModel.getPartitionValues())) {
          continue;
        }

        String subJobId = JobUtils.generateJobId(true);
        Map<String, String> subConfig = new HashMap<>(config);
        String partitionIdentifier = ConfigurationUtils.toPartitionIdentifier(
            tableName,
            partitionMetaModel.getPartitionValues());
        subConfig.put(JobConfiguration.SOURCE_OBJECT_NAME, partitionIdentifier);
        subConfig.put(JobConfiguration.DEST_OBJECT_NAME, partitionIdentifier);
        subConfig.put(JobConfiguration.OBJECT_TYPE, ObjectType.PARTITION.name());
        if (partitionMetaModel.getLastModificationTime() != null) {
          subConfig.put(
              JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME,
              Long.toString(partitionMetaModel.getLastModificationTime()));
        }

        metaManager.addSubJob(
            jobId,
            subJobId,
            jobPriority,
            jobMaxAttemptTimes,
            new JobConfiguration(subConfig).toString(),
            false);
      }
    } else {
      extraConfig.put(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME,
                    Long.toString(tableMetaModel.getLastModificationTime()));
    }

    // add extra config
    extraConfig.putAll(config);
    config = new JobConfiguration(extraConfig);

    if (isSubJob) {
      metaManager.addSubJob(
          parentJobId,
          jobId,
          jobPriority,
          jobMaxAttemptTimes,
          config.toString(),
          isPartitioned);
    } else {
      metaManager.addJob(
          jobId,
          jobPriority,
          jobMaxAttemptTimes,
          config.toString(),
          isPartitioned);
    }

    return jobId;
  }

  public synchronized void addSubJob(String parentJobId, JobConfiguration config) {
    if (!config.containsKey(JobConfiguration.JOB_ID)) {
      throw new IllegalArgumentException("Sub job id is required");
    }
    String jobId = config.get(JobConfiguration.JOB_ID);
    LOG.info("Add sub job, parent job id: {}, sub job id: {}", parentJobId, jobId);

    if (metaManager.getSubJobById(parentJobId, jobId) != null) {
      throw new IllegalArgumentException("Sub job already exists: " + parentJobId + "." + jobId);
    }

    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    String jobPriorityString = config.getOrDefault(
        JobConfiguration.JOB_PRIORITY,
        JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE);
    int jobPriority = Integer.valueOf(jobPriorityString);
    String jobMaxAttemptTimesString = config.getOrDefault(
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES,
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES_DEFAULT_VALUE);
    int jobMaxAttemptTimes = Integer.valueOf(jobMaxAttemptTimesString);
    switch (objectType) {
      case TABLE: {
        throw new IllegalArgumentException("Unsupported object type " + objectType);
      }
      case PARTITION: {
        metaManager.addSubJob(
            parentJobId,
            jobId,
            jobPriority,
            jobMaxAttemptTimes,
            config.toString(),
            false);
        break;
      }
      default:
        throw new IllegalArgumentException("Unsupported object type " + objectType);
    }
  }

  public synchronized void removeJob(String jobId) {
    LOG.info("Remove job, job id: {}", jobId);
    removeJobInternal(null, jobId);
  }

  public synchronized void removeSubJob(String parentJobId, String jobId) {
    LOG.info("Remove sub job, parent job id: {}, sub job id: {}");
    removeJobInternal(parentJobId, jobId);
  }

  private void removeJobInternal(String parentJobId, String jobId) {
    jobs.remove(jobId);
    com.aliyun.odps.mma.server.meta.generated.Job record;

    if (StringUtils.isBlank(parentJobId)) {
      record = metaManager.getJobById(jobId);
    } else {
      record = metaManager.getSubJobById(parentJobId, jobId);
    }

    if (record == null) {
      throw new IllegalArgumentException("Job does not exist, job id: " + jobId);
    }

    if (record.hasSubJob()) {
      List<com.aliyun.odps.mma.server.meta.generated.Job> subRecords =
          metaManager.listSubJobs(record.getJobId());
      for (com.aliyun.odps.mma.server.meta.generated.Job subRecord : subRecords) {
        removeJobInternal(jobId, subRecord.getJobId());
      }
    }

    if (StringUtils.isBlank(parentJobId)) {
      metaManager.removeJob(jobId);
    } else {
      metaManager.removeSubJob(parentJobId, jobId);
    }
  }

  public Job getJobById(String jobId) {
    LOG.info("Get job, job id: {}", jobId);

    com.aliyun.odps.mma.server.meta.generated.Job record =
        metaManager.getJobById(jobId);
    if (record == null) {
      throw new IllegalArgumentException("Job does not exist, job id: " + jobId);
    }
    return getJobInternal(null, record);
  }

  private Job getJobInternal(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {

    String jobId = record.getJobId();

    if (jobs.containsKey(jobId)) {
      return jobs.get(jobId);
    } else {
      LOG.info("Construct job object, job id: {}", record.getJobId());
    }

    JobConfiguration config = JobConfiguration.fromJson(record.getJobConfig());
    MetaSourceType metaSourceType = MetaSourceType.valueOf(
        config.get(JobConfiguration.METADATA_SOURCE_TYPE));
    DataSourceType dataSourceType = DataSourceType.valueOf(
        config.get(JobConfiguration.DATA_SOURCE_TYPE));
    MetaDestType metaDestType = MetaDestType.valueOf(
        config.get(JobConfiguration.METADATA_DEST_TYPE));
    DataDestType dataDestType = DataDestType.valueOf(
        config.get(JobConfiguration.DATA_DEST_TYPE));

    Job job;
    if (metaSourceType.equals(MetaSourceType.MaxCompute)
        && dataSourceType.equals(DataSourceType.MaxCompute)
        && metaDestType.equals(MetaDestType.OSS)
        && dataDestType.equals(DataDestType.OSS)) {
      job = getMcToOssJob(parentJob, config, record);
    } else if (metaSourceType.equals(MetaSourceType.OSS)
               && dataSourceType.equals(DataSourceType.OSS)
               && metaDestType.equals(MetaDestType.MaxCompute)
               && dataDestType.equals(DataDestType.MaxCompute)) {
      job = getOssToMcJob(parentJob, config, record);
    } else if (metaSourceType.equals(MetaSourceType.Hive)
               && dataSourceType.equals(DataSourceType.Hive)
               && metaDestType.equals(MetaDestType.MaxCompute)
               && dataDestType.equals(DataDestType.MaxCompute)) {
      job = getHiveToMcJob(parentJob, config, record);
    } else {
      throw new IllegalArgumentException("Unsupported source and dest combination.");
    }

    jobs.put(job.getId(), job);
    return job;
  }

  private Job getMcToOssJob(
      Job parentJob,
      JobConfiguration config,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG: {
        return getMcToOssCatalogJob(parentJob, record);
      }
      case TABLE: {
        return getMcToOssTableJob(parentJob, record);
      }
      case PARTITION: {
        return getPartitionJob(parentJob, record);
      }
      case RESOURCE: {
        return getMcToOssResourceJob(parentJob, record);
      }
      case FUNCTION: {
        return getMcToOssFunctionJob(parentJob, record);
      }
      default:
        throw new IllegalArgumentException("Unsupported object type " + objectType);
    }
  }

  private Job getOssToMcJob(
      Job parentJob,
      JobConfiguration config,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG: {
        return getOssToMcCatalogJob(parentJob, record);
      }
      case TABLE: {
        return getOssToMcTableJob(parentJob, record);
      }
      case PARTITION: {
        return getPartitionJob(parentJob, record);
      }
      case RESOURCE: {
        return getOssToMcResourceJob(parentJob, record);
      }
      case FUNCTION: {
        return getOssToMcFunctionJob(parentJob, record);
      }
      default:
        throw new IllegalArgumentException("Unsupported object type " + objectType);
    }
  }

  private Job getHiveToMcJob(
      Job parentJob,
      JobConfiguration config,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG: {
        return getHiveToMcCatalogJob(parentJob, record);
      }
      case TABLE: {
        return getHiveToMcTableJob(parentJob, record);
      }
      case PARTITION: {
        return getPartitionJob(parentJob, record);
      }
      default:
        throw new IllegalArgumentException("Unsupported object type " + objectType);
    }
  }

  private Job getHiveToMcCatalogJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new CatalogJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getHiveToMcTableJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new HiveToMcTableJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getMcToOssCatalogJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new McToOssCatalogJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getMcToOssTableJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new McToOssTableJob(
        parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getMcToOssResourceJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new McToOssResourceJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getMcToOssFunctionJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new McToOssFunctionJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getOssToMcCatalogJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new McToOssCatalogJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getOssToMcTableJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new OssToMcTableJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getOssToMcResourceJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new OssToMcResourceJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getOssToMcFunctionJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new OssToMcFunctionJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  private Job getPartitionJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record) {
    return new PartitionJob(parentJob, record, this, metaManager, metaSourceFactory);
  }

  public Job getSubJobById(Job parentJob, String subJobId) {
    LOG.info("Get sub job, parent job id: {}, job id: {}", parentJob.getId(), subJobId);
    com.aliyun.odps.mma.server.meta.generated.Job subRecord =
        metaManager.getSubJobById(parentJob.getId(), subJobId);
    if (subRecord == null) {
      throw new IllegalArgumentException("Sub job does not exist, job id: " + subJobId);
    }
    return getJobInternal(parentJob, subRecord);
  }

  public List<Job> listJobs() {
    LOG.info("List jobs");
    List<com.aliyun.odps.mma.server.meta.generated.Job> records =
        metaManager.listJobs();

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (com.aliyun.odps.mma.server.meta.generated.Job record : records) {
      ret.add(getJobInternal(null, record));
    }

    return ret;
  }

  public List<Job> listJobsByStatus(JobStatus jobStatus) {
    return listJobByStatusInternal(jobStatus);
  }

  private List<Job> listJobByStatusInternal(JobStatus jobStatus) {
    List<com.aliyun.odps.mma.server.meta.generated.Job> records =
        metaManager.listJobsByStatus(jobStatus);

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (com.aliyun.odps.mma.server.meta.generated.Job record : records) {
      ret.add(getJobInternal(null, record));
    }

    return ret;
  }

  public List<Job> listSubJobs(Job parentJob) {
    LOG.info("List sub jobs, parent job id: {}", parentJob.getId());
    List<com.aliyun.odps.mma.server.meta.generated.Job> records =
        metaManager.listSubJobs(parentJob.getId());

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (com.aliyun.odps.mma.server.meta.generated.Job record : records) {
      ret.add(getJobInternal(parentJob, record));
    }

    return ret;
  }

  public List<Job> listSubJobsByStatus(Job parentJob, JobStatus jobStatus) {
    LOG.info(
        "List sub jobs by status, parent job id: {}, status: {}",
        parentJob,
        jobStatus.name());
    List<com.aliyun.odps.mma.server.meta.generated.Job> records =
        metaManager.listSubJobsByStatus(parentJob.getId(), jobStatus);

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (com.aliyun.odps.mma.server.meta.generated.Job record : records) {
      ret.add(getJobInternal(parentJob, record));
    }

    return ret;
  }
}
