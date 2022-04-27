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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import com.aliyun.odps.mma.server.meta.generated.JobRecord;

import static com.aliyun.odps.mma.config.ObjectType.CATALOG;
import static com.aliyun.odps.mma.config.ObjectType.FUNCTION;
import static com.aliyun.odps.mma.config.ObjectType.PARTITION;
import static com.aliyun.odps.mma.config.ObjectType.RESOURCE;
import static com.aliyun.odps.mma.config.ObjectType.TABLE;

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
    JobDescribe jobDescribe = new JobDescribe(config);
    jobDescribe.check();

    boolean hasSubJob = ObjectType.CATALOG.equals(jobDescribe.objectType);
    jobId = saveJobToDb(null, config, hasSubJob, null);
    if (TABLE.equals(jobDescribe.objectType)) {
      addTableSubJobs(metaManager.getJobById(jobId), null);
    }
    return jobId;
  }

  public void addSubJobsInCatalogJob(JobRecord record) throws Exception {
    // add sub jobs first time
    removeSubJobTable(record.getJobId());
    Job job = getJobById(record.getJobId());
    HashMap<ObjectType, Set<String>> blackList =
        JobUtils.getObjectFilterList(job.getJobConfiguration().get(JobConfiguration.SOURCE_BLACKLIST));
    HashMap<ObjectType, Set<String>> whiteList=
        JobUtils.getObjectFilterList(job.getJobConfiguration().get(JobConfiguration.SOURCE_WHITELIST));
    addCatalogJob(record, blackList, whiteList);
  }

  public boolean addNewSubJobsInCatalogJob(JobRecord record) throws Exception {
    // add new sub jobs when reset catalog job
    // group existed jobs by object type
    //   table => t1,t2,t3...
    //   resource => r1,r2,r3...
    LOG.info("add new subjobs in catalog job, job id: {}", record.getJobId());
    Map<ObjectType, Set<String>> objectExistsJobs = new HashMap<>();

    Job job = getJobById(record.getJobId());
    List<Job> subJobs = listSubJobs(job);
    for (Job subjob: subJobs) {
      ObjectType objectType = ObjectType.valueOf(subjob.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE));
      if (!objectExistsJobs.containsKey(objectType)) {
        objectExistsJobs.put(objectType, new HashSet<>());
      }
      String objectName = subjob.getJobConfiguration().get(JobConfiguration.SOURCE_OBJECT_NAME);
      objectExistsJobs.get(objectType).add(objectName);
    }
    return addCatalogJob(record, objectExistsJobs, null);
  }

  public void removeInvalidSubJobsInCatalogJob(JobRecord record) throws Exception {
    // remove subjobs not in the current catalog
    Job job = getJobById(record.getJobId());
    JobConfiguration config = job.getJobConfiguration();
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);

    Map<ObjectType, Set<String>> currentObjectNames =
        getCurrentObjectNames(metaSource, config, catalogName);

    for (Job subJob : job.getSubJobs()) {
      ObjectType objectType = ObjectType.valueOf(subJob.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE));
      String objectName = subJob.getJobConfiguration().get(JobConfiguration.SOURCE_OBJECT_NAME);
      if (!currentObjectNames.get(objectType).contains(objectName)) {
        LOG.info("Remove invalid sub job, parent job id: {}, sub job id: {}",
                 record.getJobId(), subJob.getId());
        removeSubJob(record.getJobId(), subJob.getId());
      }
    }
  }

  private List<ObjectType> getSupportedObjectTypes(
      MetaSource metaSource, JobConfiguration config) throws Exception {
    List<ObjectType> objectTypes;
    List<ObjectType> supportedObjectTypes = metaSource.getSupportedObjectTypes();
    if (config.containsKey(JobConfiguration.SOURCE_OBJECT_TYPES)) {
      String[] objectTypeList = config.get(JobConfiguration.SOURCE_OBJECT_TYPES).split(",");
      objectTypes = Arrays.stream(objectTypeList).map(ObjectType::valueOf).collect(Collectors.toList());
      for (ObjectType objectType : objectTypes) {
        if (!supportedObjectTypes.contains(objectType)) {
          throw new IllegalArgumentException("Unsupported object type: " + objectType);
        }
      }
    } else {
      // If object types are not specified, use the supported object types
      objectTypes = supportedObjectTypes;
    }
    return objectTypes;
  }

  public boolean addCatalogJob(JobRecord record,
                               Map<ObjectType, Set<String>> blackList,
                               Map<ObjectType, Set<String>> whiteList) throws Exception {
    // blackList ∩ whiteList = ∅
    // priority: blackList > whiteList
    // full set     |   blackList   |   whiteList    |  result
    // [1,2,3,4]    |   null        |   null         |  [1,2,3,4]
    // [1,2,3,4]    |   null        |   [1,2,3]      |  [1,2,3]
    // [1,2,3,4]    |   [1,2,3]     |   null         |  [4]
    // [1,2,3,4]    |   [1,2,3]     |   [1]          |  throw Exception
    // [1,2,3,4]    |   [1,2,3]     |   [4]          |  [4]
    if (JobUtils.intersectionIsNotNull(blackList, whiteList)) {
      throw new IllegalArgumentException("ERROR: black list and white list intersection should be EMPTY");
    }

    // only use in update mode
    boolean addNew = false;

    String jobId = record.getJobId();
    JobConfiguration config = JobConfiguration.fromJson(record.getJobConfig());

    // Supported object types
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    List<ObjectType> objectTypes = getSupportedObjectTypes(metaSource, config);

    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    try (SqlSession session = metaManager.getSqlSessionFactory().openSession(true)) {
      Map<ObjectType, Set<String>> currentObjectNames = getCurrentObjectNames(
          metaSource, config, catalogName);
      for (Map.Entry<ObjectType, Set<String>> entry : currentObjectNames.entrySet()) {
        ObjectType objectType = entry.getKey();
        Set<String> objectNames = entry.getValue();
        Set<String> blackListNames = new HashSet<String>();
        Set<String> whiteListNames = new HashSet<>();
        if (blackList != null && blackList.containsKey(objectType)) {
          blackListNames = blackList.get(objectType);
        }
        if (whiteList != null && whiteList.containsKey(objectType)) {
          whiteListNames = whiteList.get(objectType);
        }

        LOG.info(objectNames);
        for (String objName : objectNames) {
          if (blackListNames.contains(objName)) {
            continue;
          }
          if (!whiteListNames.contains(objName)) {
            continue;
          }
          addNew = true;
          LOG.info("add {} job, object name: {}", objectType, objName);
          String subJobId = saveSubJobToDb(jobId, config, objName, objectType, null, session);
          if (ObjectType.TABLE.equals(objectType)) {
            JobRecord subRecord = metaManager.getSubJobById(jobId, subJobId);
            addTableSubJobs(subRecord, jobId);
          }
        }
      }
    }

    Map<String, String> newConfig = new HashMap<>(config);
    newConfig.put(JobConfiguration.PLAN_INIT, "1");
    record.setJobConfig(new JobConfiguration(newConfig).toString());
    metaManager.updateJobById(record);
    return addNew;
  }

  Map<ObjectType, Set<String>> getCurrentObjectNames(
      MetaSource metaSource, JobConfiguration config, String catalogName) throws Exception {
    Map<ObjectType, Set<String>> currentObjects = new HashMap<>();
    List<ObjectType> objectTypes = getSupportedObjectTypes(metaSource, config);
    for (ObjectType objectType : objectTypes) {
      if (ObjectType.TABLE.equals(objectType)) {
        currentObjects.put(objectType, new HashSet<>(metaSource.listTables(catalogName)));
      } else if (ObjectType.RESOURCE.equals(objectType)) {
        currentObjects.put(objectType, new HashSet<>(metaSource.listResources(catalogName)));
      } else if (ObjectType.FUNCTION.equals(objectType)) {
        currentObjects.put(objectType, new HashSet<>(metaSource.listFunctions(catalogName)));
      } else {
        throw new IllegalArgumentException("Unsupported object type " + objectType);
      }
    }
    return currentObjects;
  }

  public String saveSubJobToDb(String parentJobId, JobConfiguration config,
                               String objName, ObjectType objectType,
                               Long lastModifiedTime, SqlSession session) {
    Map<String, String> subConfig = new HashMap<>(config);
    subConfig.put(JobConfiguration.SOURCE_OBJECT_NAME, objName);
    subConfig.put(JobConfiguration.DEST_OBJECT_NAME, objName);
    subConfig.put(JobConfiguration.OBJECT_TYPE, objectType.name());
    if (lastModifiedTime != null) {
      subConfig.put(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME, Long.toString(lastModifiedTime));
    }
    return saveJobToDb(parentJobId, new JobConfiguration(subConfig), false, session);
  }

  public String saveJobToDb(
      String parentJobId,
      JobConfiguration config,
      boolean hasSubJob,
      SqlSession session
      ) {
    // add root job: parentJobId == null
    // add subjob: parentJobId == null
    //      jobId in config is parentJobId
    //      jobId in db must generate like S_xxx

    boolean isSubJob = !StringUtils.isBlank(parentJobId);
    String jobId = config.get(JobConfiguration.JOB_ID);

    //  isSubJob  hasJobIdInConfig    jobId
    //  1         0/1                 generate => S_xxx
    //  0         0                   generate => xxx
    //  0         1                   use job_id in config
    if (isSubJob || StringUtils.isBlank(jobId)) {
      jobId = JobUtils.generateJobId(isSubJob);
    }

    String jobPriorityString = config.getOrDefault(
        JobConfiguration.JOB_PRIORITY,
        JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE);
    int jobPriority = Integer.parseInt(jobPriorityString);
    String jobMaxAttemptTimesString = config.getOrDefault(
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES,
        JobConfiguration.JOB_MAX_ATTEMPT_TIMES_DEFAULT_VALUE);
    int jobMaxAttemptTimes = Integer.parseInt(jobMaxAttemptTimesString);


    if (isSubJob) {
      // overwrite parent jobid
      Map<String, String> subConfig = new HashMap<>(config);
      subConfig.put(JobConfiguration.JOB_ID, jobId);
      config = new JobConfiguration(subConfig);
      if (session != null) {
        metaManager.addSubJob(
            parentJobId,
            jobId,
            jobPriority,
            jobMaxAttemptTimes,
            config.toString(),
            hasSubJob,
            session);
      } else {
        metaManager.addSubJob(
            parentJobId,
            jobId,
            jobPriority,
            jobMaxAttemptTimes,
            config.toString(),
            hasSubJob);
      }
    } else {
      metaManager.addJob(
          jobId,
          jobPriority,
          jobMaxAttemptTimes,
          config.toString(),
          hasSubJob);
    }
    return jobId;
  }

  private void addTableSubJobs(JobRecord record, String parentJobId) throws Exception {
    // update tablejob lastModifiedTime(in config) and hasSubJob(partition)
    // add subjobs

    JobConfiguration config = JobConfiguration.fromJson(record.getJobConfig());
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String tableName = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
    String jobId = record.getJobId();
    removeSubJobTable(jobId);

    // Ignore schema name for now, since MetaSource interface doesn't support it.
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    TableMetaModel tableMetaModel = metaSource.getTableMeta(catalogName, tableName);
    boolean isPartitioned = !tableMetaModel.getPartitionColumns().isEmpty();

    if (isPartitioned) {
      try (SqlSession session = metaManager.getSqlSessionFactory().openSession(true)) {
        // Add each partition as a sub job
        JobUtils.PartitionFilter partitionFilter = new JobUtils.PartitionFilter(config);
        for (PartitionMetaModel partitionMetaModel : tableMetaModel.getPartitions()) {

          if (!partitionFilter.filter(partitionMetaModel.getPartitionValues())) {
            continue;
          }

          String partitionIdentifier = ConfigurationUtils.toPartitionIdentifier(
                  tableName,
                  partitionMetaModel.getPartitionValues());
          saveSubJobToDb(jobId, config, partitionIdentifier, PARTITION,
                         partitionMetaModel.getLastModificationTime(), session);
        }
      }
    } else {
      if (tableMetaModel.getLastModificationTime() != null) {
        Map<String, String> configMap = new HashMap<>(config);
        configMap.put(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME,
                      Long.toString(tableMetaModel.getLastModificationTime()));
        config = new JobConfiguration(configMap);
        record.setJobConfig(config.toString());
      }
    }

    record.setHasSubJob(isPartitioned);
    if (parentJobId == null) {
      metaManager.updateJobById(record);
    } else {
      metaManager.updateSubJobById(parentJobId, record);
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

  public synchronized void removeSubJobTable(String parentJobId) {
    metaManager.createSubJobTable(parentJobId);
    for(JobRecord jobRecord : metaManager.listSubJobs(parentJobId)) {
      metaManager.removeSubJobTable(jobRecord.getJobId());
    }
    metaManager.removeSubJobTable(parentJobId);
    metaManager.createSubJobTable(parentJobId);
    LOG.info("Remove sub job table, parent job id: {}", parentJobId);
  }

  private void removeJobInternal(String parentJobId, String jobId) {
    jobs.remove(jobId);
    JobRecord record;

    if (StringUtils.isBlank(parentJobId)) {
      record = metaManager.getJobById(jobId);
    } else {
      record = metaManager.getSubJobById(parentJobId, jobId);
    }

    if (record == null) {
      throw new IllegalArgumentException("Job does not exist, job id: " + jobId);
    }

    if (record.hasSubJob()) {
      List<JobRecord> subRecords = metaManager.listSubJobs(record.getJobId());
      for (JobRecord subRecord : subRecords) {
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
     return getJobById(jobId, false);
  }

  public Job getJobById(String jobId, boolean refreshFromDb) {
    LOG.info("Get job, job id: {}", jobId);

    JobRecord record = metaManager.getJobById(jobId);
    if (record == null) {
      throw new IllegalArgumentException("Job does not exist, job id: " + jobId);
    }
    return getJobInternal(null, record, refreshFromDb);
  }

  static class JobDescribe {
    MetaSourceType metaSourceType;
    DataSourceType dataSourceType;
    MetaDestType metaDestType;
    DataDestType dataDestType;
    ObjectType objectType;

    public JobDescribe(JobConfiguration config) {
      metaSourceType = MetaSourceType.valueOf(config.get(JobConfiguration.METADATA_SOURCE_TYPE));
      dataSourceType = DataSourceType.valueOf(config.get(JobConfiguration.DATA_SOURCE_TYPE));
      metaDestType = MetaDestType.valueOf(config.get(JobConfiguration.METADATA_DEST_TYPE));
      dataDestType = DataDestType.valueOf(config.get(JobConfiguration.DATA_DEST_TYPE));
      objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    }

    boolean isMcToOSSJob() {
      return metaSourceType.equals(MetaSourceType.MaxCompute) && dataSourceType.equals(DataSourceType.MaxCompute)
          && metaDestType.equals(MetaDestType.OSS) && dataDestType.equals(DataDestType.OSS);
    }

    boolean isOSSToMcJob() {
      return metaSourceType.equals(MetaSourceType.OSS) && dataSourceType.equals(DataSourceType.OSS)
          && metaDestType.equals(MetaDestType.MaxCompute) && dataDestType.equals(DataDestType.MaxCompute);
    }

    boolean isHiveToMcJob() {
      return metaSourceType.equals(MetaSourceType.Hive) && dataSourceType.equals(DataSourceType.Hive)
          && metaDestType.equals(MetaDestType.MaxCompute) && dataDestType.equals(DataDestType.MaxCompute);
    }

    void check() {
      Set<ObjectType> mcOSSValidType = new HashSet<>(Arrays.asList(CATALOG, TABLE, FUNCTION, RESOURCE));
      Set<ObjectType> hiveMcValidType = new HashSet<>(Arrays.asList(CATALOG, TABLE));
      boolean isValidMcToOSSJob = isMcToOSSJob() && mcOSSValidType.contains(objectType);
      boolean isValidOSSToMcJob = isOSSToMcJob() && mcOSSValidType.contains(objectType);
      boolean isValidHiveToMcJob = isHiveToMcJob() && hiveMcValidType.contains(objectType);
      if (!isValidMcToOSSJob && !isValidOSSToMcJob && !isValidHiveToMcJob) {
        throw new IllegalArgumentException("Unsupported source and dest combination");
      }
    }
  }

  private Job getJobInternal(Job parentJob, JobRecord record) {
    return getJobInternal(parentJob, record, false);
  }

  private Job getJobInternal(Job parentJob, JobRecord record, boolean refresh) {

    String jobId = record.getJobId();

    if (!refresh && jobs.containsKey(jobId)) {
      return jobs.get(jobId);
    }

    LOG.info("Construct job object, job id: {}", record.getJobId());

    JobConfiguration config = JobConfiguration.fromJson(record.getJobConfig());
    JobDescribe jobDescribe = new JobDescribe(config);
    ObjectType objectType = jobDescribe.objectType;

    Job job;
    if (jobDescribe.isMcToOSSJob()) {
      if (CATALOG.equals(objectType)) {
        job = new McToOssCatalogJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (TABLE.equals(objectType)) {
        job = new McToOssTableJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (PARTITION.equals(objectType)) {
        job = new PartitionJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (RESOURCE.equals(objectType)) {
        job = new McToOssResourceJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (FUNCTION.equals(objectType)) {
        job = new McToOssFunctionJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else {
        throw new IllegalArgumentException("Unsupported object type " + jobDescribe.objectType);
      }
    } else if (jobDescribe.isOSSToMcJob()) {
      if (CATALOG.equals(objectType)) {
        job = new McToOssCatalogJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (TABLE.equals(objectType)) {
        job = new OssToMcTableJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (PARTITION.equals(objectType)) {
        job = new PartitionJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (RESOURCE.equals(objectType)) {
        job = new OssToMcResourceJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (FUNCTION.equals(objectType)) {
        job = new OssToMcFunctionJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else {
        throw new IllegalArgumentException("Unsupported object type " + jobDescribe.objectType);
      }
    } else if (jobDescribe.isHiveToMcJob()) {
      if (CATALOG.equals(objectType)) {
        job = new CatalogJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (TABLE.equals(objectType)) {
        job = new HiveToMcTableJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else if (PARTITION.equals(objectType)) {
        job = new PartitionJob(parentJob, record, this, metaManager, metaSourceFactory);
      } else {
          throw new IllegalArgumentException("Unsupported object type " + jobDescribe.objectType);
      }
    } else {
      throw new IllegalArgumentException("Unsupported source and dest combination.");
    }

    jobs.put(job.getId(), job);
    return job;
  }

  public Job getSubJobById(Job parentJob, String subJobId) {
    LOG.info("Get sub job, parent job id: {}, job id: {}", parentJob.getId(), subJobId);
    JobRecord subRecord = metaManager.getSubJobById(parentJob.getId(), subJobId);
    if (subRecord == null) {
      throw new IllegalArgumentException("Sub job does not exist, job id: " + subJobId);
    }
    return getJobInternal(parentJob, subRecord);
  }

  public List<Job> listJobs() {
    LOG.info("List jobs");
    List<JobRecord> records = metaManager.listJobs();

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (JobRecord record : records) {
      ret.add(getJobInternal(null, record));
    }

    return ret;
  }

  public List<Job> listJobsByStatus(JobStatus jobStatus) {
    return listJobByStatusInternal(jobStatus);
  }

  private List<Job> listJobByStatusInternal(JobStatus jobStatus) {
    List<JobRecord> records = metaManager.listJobsByStatus(jobStatus);

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (JobRecord record : records) {
      ret.add(getJobInternal(null, record));
    }

    return ret;
  }

  public List<Job> listSubJobs(Job parentJob) {
    LOG.info("List sub jobs, parent job id: {}", parentJob.getId());
    List<JobRecord> records = metaManager.listSubJobs(parentJob.getId());

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (JobRecord record : records) {
      ret.add(getJobInternal(parentJob, record));
    }

    return ret;
  }

  public List<Job> listSubJobsByStatus(Job parentJob, JobStatus jobStatus) {
    LOG.info(
        "List sub jobs by status, parent job id: {}, status: {}",
        parentJob,
        jobStatus.name());
    List<JobRecord> records = metaManager.listSubJobsByStatus(parentJob.getId(), jobStatus);

    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    List<Job> ret = new ArrayList<>(records.size());
    for (JobRecord record : records) {
      ret.add(getJobInternal(parentJob, record));
    }

    return ret;
  }
}
