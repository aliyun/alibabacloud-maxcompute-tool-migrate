package com.aliyun.odps.datacarrier.taskscheduler.ui.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.crypto.NoSuchPaddingException;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfigUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.TaskScheduler;
import com.aliyun.odps.datacarrier.taskscheduler.config.JobInfoOutputsV1;
import com.aliyun.odps.datacarrier.taskscheduler.config.OutputsWrapper;
import com.aliyun.odps.datacarrier.taskscheduler.config.ProjectBackupJobInputsV1;
import com.aliyun.odps.datacarrier.taskscheduler.config.ProjectRestoreJobInputsV1;
import com.aliyun.odps.datacarrier.taskscheduler.job.utils.JobUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.OdpsMetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.task.Task;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class JobApi extends AbstractRestfulApi {
  private static final Logger LOG = LogManager.getLogger(JobApi.class);

  private static final String PARAM_JOB_ID = "jobid";
  private static final String PARAM_PERMANENT = "permanent";
  private static final String PARAM_JOB_TYPE = "jobtype";
  private static final String PARAM_OBJECT_TYPE = "objecttype";

  private TaskScheduler taskScheduler;

  private MmaMetaManager mmaMetaManager;

  public JobApi(String prefix, MmaMetaManager mmaMetaManager, TaskScheduler taskScheduler) throws MmaException {
    super(prefix);
    this.mmaMetaManager = Objects.requireNonNull(mmaMetaManager);
    this.taskScheduler = Objects.requireNonNull(taskScheduler);
  }

  @Override
  public String handleDelete(HttpServletRequest request) throws ServletException, IOException {
    String parameterJobId = request.getParameter(PARAM_JOB_ID);
    String parameterPermanent = request.getParameter(PARAM_PERMANENT);
    if (parameterJobId == null) {
      throw new ServletException("Missing required parameter " + PARAM_JOB_ID);
    }

    boolean permanent = false;
    if (parameterPermanent != null) {
      permanent = Boolean.valueOf(parameterPermanent);
    }

    if (permanent) {
      try {
        this.mmaMetaManager.removeJob(parameterJobId);
      } catch (MmaException e) {
        throw new ServletException(e);
      }
    }

    try {
      List<Task> runningTasks = this.taskScheduler.getRunningTasks();
      for (Task t : runningTasks) {
        if (parameterJobId.equals(t.getJobId())) {
          t.stop();
        }
      }
    } catch (MmaException e) {
      throw new ServletException(e);
    }

    return null;
  }

  @Override
  public String handleGet(HttpServletRequest request) throws ServletException {
    String parameterJobId = request.getParameter(PARAM_JOB_ID);
    if (parameterJobId == null) {
      throw new ServletException("Missing required parameter " + PARAM_JOB_ID);
    }

    try {
      List<MmaMetaManagerDbImplUtils.JobInfo> total =
          this.mmaMetaManager.listMigrationJobs(-1);
      List<MmaMetaManagerDbImplUtils.JobInfo> jobInfos = total
          .stream()
          .filter(jobInfo -> parameterJobId.equals(jobInfo.getJobId()))
          .collect(Collectors.toList());

      if (jobInfos.size() == 0) {
        throw new ServletException("Job does not exist: " + parameterJobId);
      }

      return getJobInfoOutputs(parameterJobId, jobInfos);
    } catch (MmaException e) {
      throw new ServletException(e);
    }
  }

  public String getJobInfoOutputs(String jobId, List<MmaMetaManagerDbImplUtils.JobInfo> jobInfos) {
    OutputsWrapper<JobInfoOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);

    JobInfoOutputsV1 outputs = new JobInfoOutputsV1();
    outputs.setJobId(jobId);

    Map<MmaMetaManager.JobStatus, Long> jobStatusToCount = jobInfos
        .stream()
        .collect(Collectors.groupingBy(MmaMetaManagerDbImplUtils.JobInfo::getStatus, Collectors.counting()));

    boolean terminated = jobStatusToCount
        .entrySet()
        .stream()
        .allMatch(e -> MmaMetaManager.JobStatus.SUCCEEDED.equals(e.getKey()) || MmaMetaManager.JobStatus.FAILED.equals(e.getKey()));

    if (terminated) {
      boolean isSucceeded = !jobStatusToCount.containsKey(MmaMetaManager.JobStatus.FAILED);
      if (isSucceeded) {
        outputs.setStatus(MmaMetaManager.JobStatus.SUCCEEDED.name());
      } else {
        outputs.setStatus(MmaMetaManager.JobStatus.FAILED.name());
      }
      outputs.setProgress(100.0D);
    } else {
      outputs.setStatus(MmaMetaManager.JobStatus.RUNNING.name());
      long numSucceededJob = jobStatusToCount.getOrDefault(MmaMetaManager.JobStatus.SUCCEEDED, 0L);
      double progress = numSucceededJob / jobInfos.size() * 100.0D;
      outputs.setProgress(progress);
    }

    wrapper.setOutputs(outputs);
    return GSON.toJson(wrapper);
  }

  @Override
  public String handlePost(HttpServletRequest request) throws ServletException, IOException {
    JsonElement decryptedInputs;
    String parameterJobType = request.getParameter(PARAM_JOB_TYPE);
    if (parameterJobType == null) {
      throw new ServletException("Missing required parameter " + PARAM_JOB_TYPE);
    }
    String parameterObjectType = request.getParameter(PARAM_OBJECT_TYPE);
    if (parameterObjectType == null) {
      throw new ServletException("Missing required parameter " + PARAM_OBJECT_TYPE);
    }

    LOG.debug("Job type: {}, Object type: ", parameterJobType, parameterObjectType);

    ServletInputStream im = request.getInputStream();
    byte[] bytes = IOUtils.toByteArray(im);
    JsonParser parser = new JsonParser();
    JsonObject body = parser.parse(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();

    if (!body.has("ProtocolVersion")) {
      throw new ServletException("Missing required member 'ProtocolVersion'");
    }
    int protocolVersion = body.get("ProtocolVersion").getAsInt();
    LOG.debug("Protocol version: {}", protocolVersion);


    try {
      decryptedInputs = getDecryptedInputs(body);
    } catch (NoSuchPaddingException
        | java.security.NoSuchAlgorithmException
        | java.security.InvalidKeyException
        | javax.crypto.BadPaddingException
        | javax.crypto.IllegalBlockSizeException e) {
      throw new ServletException(e);
    }


    if (MmaConfig.JobType.BACKUP.name().equalsIgnoreCase(parameterJobType)) {
      if (protocolVersion == 1) {
        if (MmaConfig.ObjectType.DATABASE.name().equalsIgnoreCase(parameterObjectType)) {

          try {
            ProjectBackupJobInputsV1 inputs =
                GSON.fromJson(decryptedInputs, ProjectBackupJobInputsV1.class);
            return addProjectBackupJob(inputs);
          } catch (MmaException e) {
            throw new ServletException(e);
          }
        }
        throw new ServletException("Unsupported object type: " + parameterObjectType);
      }

      throw new ServletException("Unsupported protocol version: " + protocolVersion);
    }
    if (MmaConfig.JobType.RESTORE.name().equalsIgnoreCase(parameterJobType)) {
      if (protocolVersion == 1) {
        if (MmaConfig.ObjectType.DATABASE.name().equalsIgnoreCase(parameterObjectType)) {

          try {
            ProjectRestoreJobInputsV1 inputs =
                GSON.fromJson(decryptedInputs, ProjectRestoreJobInputsV1.class);
            return addProjectRestoreJob(inputs);
          } catch (MmaException e) {
            throw new ServletException(e);
          }
        }
        throw new ServletException("Unsupported object type: " + parameterObjectType);
      }

      throw new ServletException("Unsupported job type: " + parameterJobType);
    }  if (MmaConfig.JobType.MIGRATION.name().equalsIgnoreCase(parameterJobType))
    {
      throw new ServletException("Unsupported job type: " + parameterJobType);
    }
    throw new ServletException("Unsupported job type: " + parameterJobType);
  }

  private MmaConfig.ObjectBackupConfig getObjectBackupConfig(
      String sourceProjectName,
      String objectName,
      MmaConfig.ObjectType objectType,
      String backupName,
      ProjectBackupJobInputsV1 jobConfig) {

    MmaConfig.ObjectBackupConfig objectBackupConfig = new MmaConfig.ObjectBackupConfig(
        jobConfig.getDataSource(),
        sourceProjectName,
        objectName,
        objectType,
        backupName,
        MmaConfigUtils.DEFAULT_ADDITIONAL_TABLE_CONFIG);
    MmaConfig.OdpsConfig odpsConfig = new MmaConfig.OdpsConfig(
        jobConfig.getOdpsAccessKeyId(),
        jobConfig.getOdpsAccessKeySecret(),
        jobConfig.getOdpsEndpoint(),
        sourceProjectName);
    MmaConfig.OssConfig ossConfig = new MmaConfig.OssConfig(
        jobConfig.getOssEndpoint(),
        jobConfig.getOssLocalEndpoint(),
        jobConfig.getOssBucket(),
        jobConfig.getOssRoleArn(),
        jobConfig.getOssAccessKeyId(),
        jobConfig.getOssAccessKeySecret());
    objectBackupConfig.setOdpsConfig(odpsConfig);
    objectBackupConfig.setOssConfig(ossConfig);
    return objectBackupConfig;
  }

  private void addObjectBackupJob(
      String jobId,
      ProjectBackupJobInputsV1 jobConfig,
      MmaConfig.ObjectType objectType,
      String objectName) throws MmaException {
    LOG.info("Add object backup job, backup name: {}, project:{}, object type: {}, object name: {}",
             jobConfig.getBackupName(), jobConfig.getSourceProjectName(), objectType, objectName);

    MmaConfig.ObjectBackupConfig objectBackupConfig = getObjectBackupConfig(
        jobConfig.getSourceProjectName(),
        objectName,
        objectType,
        jobConfig.getBackupName(), jobConfig);

    this.mmaMetaManager.addObjectBackupJob(jobId, objectBackupConfig);
  }


  private String addProjectBackupJob(ProjectBackupJobInputsV1 jobConfig) throws MmaException, ServletException {
    jobConfig.validate();

    LOG.info("Add project backup job, backup name: {}, project: {}",
             jobConfig.getBackupName(),
             jobConfig.getSourceProjectName());

    OdpsMetaSource metaSource = new OdpsMetaSource(
        jobConfig.getOdpsAccessKeyId(),
        jobConfig.getOdpsAccessKeySecret(),
        jobConfig.getOdpsEndpoint(),
        jobConfig.getSourceProjectName());

    String jobId = JobUtils.generateJobId();
    if (!StringUtils.isNullOrEmpty(jobConfig.getJobId())) {
      jobId = jobConfig.getJobId();
    }

    for (MmaConfig.ObjectType objectType : jobConfig.getObjectTypes()) {
      switch (objectType) {
        case TABLE:
          for (String tableName : metaSource.listTables(jobConfig.getSourceProjectName())) {
            if (!tableName.startsWith("_temporary_table_generated_by_mma_")) {
              addObjectBackupJob(jobId, jobConfig, MmaConfig.ObjectType.TABLE, tableName);
            }
          }
          continue;
        case VIEW:
          for (String viewName : metaSource.listViews(jobConfig.getSourceProjectName())) {
            addObjectBackupJob(jobId, jobConfig, MmaConfig.ObjectType.VIEW, viewName);
          }
          continue;
        case RESOURCE:
          for (String resourceName : metaSource.listResources(jobConfig.getSourceProjectName())) {
            addObjectBackupJob(jobId, jobConfig, MmaConfig.ObjectType.RESOURCE, resourceName);
          }
          continue;
        case FUNCTION:
          for (String functionName : metaSource.listFunctions(jobConfig.getSourceProjectName())) {
            addObjectBackupJob(jobId, jobConfig, MmaConfig.ObjectType.FUNCTION, functionName);
          }
          continue;
        default:
          throw new ServletException("Unsupported object type: " + objectType);
      }
    }

    OutputsWrapper<JobInfoOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    JobInfoOutputsV1 outputs = new JobInfoOutputsV1();
    outputs.setJobId(jobId);
    outputs.setStatus(MmaMetaManager.JobStatus.RUNNING.name());
    outputs.setProgress(0.0D);
    wrapper.setOutputs(outputs);

    return GSON.toJson(wrapper);
  }

  private MmaConfig.DatabaseRestoreConfig getDatabaseRestoreConfig(ProjectRestoreJobInputsV1 jobConfig) {
    MmaConfig.DatabaseRestoreConfig databaseRestoreConfig = new MmaConfig.DatabaseRestoreConfig(
        jobConfig.getSourceProjectName(),
        jobConfig.getDestProjectName(),
        jobConfig.getObjectTypes(),
        (jobConfig.getUpdateIfExists() == null) || jobConfig.getUpdateIfExists(),
        jobConfig.getBackupName(),
        jobConfig.getSettings());
    MmaConfig.OdpsConfig odpsConfig = new MmaConfig.OdpsConfig(
        jobConfig.getOdpsAccessKeyId(),
        jobConfig.getOdpsAccessKeySecret(),
        jobConfig.getOdpsEndpoint(),
        jobConfig.getDestProjectName());
    MmaConfig.OssConfig ossConfig = new MmaConfig.OssConfig(
        jobConfig.getOssEndpoint(),
        jobConfig.getOssLocalEndpoint(),
        jobConfig.getOssBucket(),
        jobConfig.getOssRoleArn(),
        jobConfig.getOssAccessKeyId(),
        jobConfig.getOssAccessKeySecret());
    databaseRestoreConfig.setOdpsConfig(odpsConfig);
    databaseRestoreConfig.setOssConfig(ossConfig);

    databaseRestoreConfig.setAdditionalTableConfig(MmaConfigUtils.DEFAULT_ADDITIONAL_TABLE_CONFIG);
    return databaseRestoreConfig;
  }

  private String addProjectRestoreJob(ProjectRestoreJobInputsV1 jobConfig) throws MmaException {
    jobConfig.validate();

    LOG.info("Add project restore job, backup name: {}, src project: {}, desc project: {}",
             jobConfig.getBackupName(),
             jobConfig.getSourceProjectName(),
             jobConfig.getDestProjectName());

    String jobId = JobUtils.generateJobId();
    if (!StringUtils.isNullOrEmpty(jobConfig.getJobId())) {
      jobId = jobConfig.getJobId();
    }

    this.mmaMetaManager.addDatabaseRestoreJob(jobId, getDatabaseRestoreConfig(jobConfig));

    OutputsWrapper<JobInfoOutputsV1> wrapper = new OutputsWrapper<JobInfoOutputsV1>();
    wrapper.setProtocolVersion(1);
    JobInfoOutputsV1 outputs = new JobInfoOutputsV1();
    outputs.setJobId(jobId);
    outputs.setStatus(MmaMetaManager.JobStatus.RUNNING.name());
    outputs.setProgress(0.0D);
    wrapper.setOutputs(outputs);

    return GSON.toJson(wrapper);
  }
}
