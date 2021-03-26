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

package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.RestoreJobInfo;

public class OdpsDatabaseRestoreAction extends DefaultAction {
  private static final Logger LOG = LogManager.getLogger(OdpsDatabaseRestoreAction.class);

  private MmaConfig.ObjectType type;
  private MmaConfig.DatabaseRestoreConfig restoreConfig;
  private MmaMetaManager mmaMetaManager;
  private int limit = 100;

  private static List<MmaMetaManager.JobStatus> ALL_STATUS = new ArrayList<>();
  private static List<MmaMetaManager.JobStatus> ACTIVE_STATUS = new ArrayList<>();
  static  {
    ALL_STATUS.addAll(Arrays.asList(MmaMetaManager.JobStatus.values()));
    ACTIVE_STATUS.add(MmaMetaManager.JobStatus.PENDING);
    ACTIVE_STATUS.add(MmaMetaManager.JobStatus.RUNNING);
  }

  public OdpsDatabaseRestoreAction(
      String id,
      MmaConfig.ObjectType type,
      MmaConfig.DatabaseRestoreConfig restoreConfig,
      MmaMetaManager mmaMetaManager) {
    super(id);
    this.type = type;
    this.restoreConfig = restoreConfig;
    this.mmaMetaManager = mmaMetaManager;
  }

  @Override
  public Object call() throws MmaException {
    mergeJobInfoIntoRestoreDB();
    waitForTasksFinish();
    return null;
  }

  private void mergeJobInfoIntoRestoreDB() throws MmaException {
    if (!restoreConfig.getObjectTypes().contains(type)) {
      LOG.info("Action {} skipped {} in database restore {}",
               id, type, actionExecutionContext.getJobId());
      return;
    }
    List<String> allOssObjects = getOssObjectList();
    LOG.info("MergeDatabaseRestoreMeta db {}, type {}, all oss objects {}",
             restoreConfig.getSourceDatabaseName(), type, GsonUtils.toJson(allOssObjects));

    List<MmaMetaManagerDbImplUtils.RestoreJobInfo> recoveredObjects =
        mmaMetaManager.listRestoreJobs(getWhereConditionStatement(ALL_STATUS), -1);

    // include SUCCEEDED and FAILED
    Set<String> finishedObjects = new HashSet<>();

    // key: object, value: attempts, include PENDING and RUNNING
    Map<String, Integer> activeObjects = new HashMap<>();

    for (MmaMetaManagerDbImplUtils.RestoreJobInfo jobInfo : recoveredObjects) {
      MmaMetaManager.JobStatus status = jobInfo.getStatus();
      if (MmaMetaManager.JobStatus.SUCCEEDED.equals(status)
          || MmaMetaManager.JobStatus.FAILED.equals(status)) {
        finishedObjects.add(jobInfo.getObject());
        continue;
      }
      activeObjects.put(jobInfo.getObject(), jobInfo.getAttemptTimes());
    }

    List<MmaMetaManagerDbImplUtils.RestoreJobInfo> jobs = new ArrayList<>();
    for (String objectName : allOssObjects) {
      if (finishedObjects.contains(objectName)) {
        continue;
      }

      int attemptTimes = activeObjects.containsKey(objectName) ?
          (activeObjects.get(objectName).intValue() + 1) : 0;

      MmaConfig.JobConfig jobConfig = new MmaConfig.JobConfig(
          getJobDescription(objectName, type),
          restoreConfig.getAdditionalTableConfig());
      jobs.add(
          new RestoreJobInfo(
              actionExecutionContext.getJobId(),
              type.name(),
              restoreConfig.getSourceDatabaseName(),
              objectName,
              jobConfig,
              MmaMetaManager.JobStatus.PENDING,
              attemptTimes,
              System.currentTimeMillis()));
    }
    for (MmaMetaManagerDbImplUtils.RestoreJobInfo jobInfo : jobs) {
      mmaMetaManager.mergeJobInfoIntoRestoreDB(jobInfo);
    }
  }

  private void waitForTasksFinish() throws MmaException {
    String originDatabase = restoreConfig.getSourceDatabaseName();
    String destinationDatabase = restoreConfig.getDestinationDatabaseName();

    try {
      while (true) {
        List<RestoreJobInfo> activeTasks = mmaMetaManager.listRestoreJobs(
            getWhereConditionStatement(ACTIVE_STATUS), limit);

        List<RestoreJobInfo> failedTasks = mmaMetaManager.listRestoreJobs(
            getWhereConditionStatement(MmaMetaManager.JobStatus.FAILED), limit);

        if (activeTasks.isEmpty()) {
          if (failedTasks.isEmpty()) {
            LOG.info("Action {} from {} to {} finished, type {}",
                     id, originDatabase, destinationDatabase, type);
            return;
          }
          LOG.error("{} Wait database restore {} from {} to {} failed, failed tasks: {}",
                    id, type, originDatabase, destinationDatabase,
                    failedTasks
                        .stream()
                        .map(k -> k.getObject() + ", attempts: " + k.getAttemptTimes() + "\n")
                        .collect(Collectors.toList()));
          throw new MmaException("Restore database failed " + GsonUtils.toJson(restoreConfig));
        }
        LOG.info("{} restore database {} from {} to {}, active tasks: {}",
                 id, type, originDatabase, destinationDatabase,
                 activeTasks
                     .stream()
                     .map(k -> k.getObject() + "\n").collect(Collectors.toList()));
        Thread.sleep(10000L);
      }
    } catch (Exception e) {
      throw new MmaException("Restore database failed " + GsonUtils.toJson(restoreConfig), e);
    }
  }

  private String getJobDescription(String object, MmaConfig.ObjectType type) {
    MmaConfig.ObjectRestoreConfig objectRestoreConfig = new MmaConfig.ObjectRestoreConfig(
        restoreConfig.getSourceDatabaseName(),
        restoreConfig.getDestinationDatabaseName(),
        object,
        type,
        restoreConfig.isUpdate(),
        restoreConfig.getBackupName(),
        restoreConfig.getSettings());
    objectRestoreConfig.setOdpsConfig(restoreConfig.getOdpsConfig());
    objectRestoreConfig.setOssConfig(restoreConfig.getOssConfig());
    return GsonUtils.toJson(objectRestoreConfig);
  }

  private String getWhereConditionStatement(MmaMetaManager.JobStatus status) {
    StringBuilder builder = new StringBuilder("WHERE ");
    builder.append(String.format(
        "%s='%s'\n",
        Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID,
        actionExecutionContext.getJobId()));
    builder.append(String.format(
        "AND %s='%s'\n",
        Constants.MMA_OBJ_RESTORE_COL_DB_NAME,
        restoreConfig.getSourceDatabaseName()));
    builder.append(String.format(
        "AND %s='%s'\n",
        Constants.MMA_OBJ_RESTORE_COL_OBJECT_TYPE,
        type.toString()));
    builder.append(String.format(
        "AND %s='%s'\n",
        Constants.MMA_OBJ_RESTORE_COL_STATUS,
        status));
    return builder.toString();
  }

  private String getWhereConditionStatement(List<MmaMetaManager.JobStatus> status) {
    if (status.size() == 1) {
      return getWhereConditionStatement(status.get(0));
    }
    StringBuilder builder = new StringBuilder("WHERE ");
    builder.append(String.format(
        "%s='%s'\n",
        Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID,
        actionExecutionContext.getJobId()));
    builder.append(String.format(
        "AND %s='%s'\n",
        Constants.MMA_OBJ_RESTORE_COL_DB_NAME,
        restoreConfig.getSourceDatabaseName()));
    builder.append(String.format(
        "AND %s='%s'\n",
        Constants.MMA_OBJ_RESTORE_COL_OBJECT_TYPE,
        type.toString()));
    builder.append(String.format("AND %s in (", Constants.MMA_OBJ_RESTORE_COL_STATUS));
    for (int index = 0; index < status.size(); index++) {
      builder.append(String.format("%s'%s'", (index == 0) ? "" : ",", status.get(index)));
    }
    builder.append(")\n");
    return builder.toString();
  }

  private List<String> getOssObjectList() throws MmaException {
    String delimiter = "/";
    String folder;
    switch (type) {
      case TABLE:
        folder = Constants.EXPORT_TABLE_FOLDER;
        break;
      case VIEW:
        folder = Constants.EXPORT_VIEW_FOLDER;
        break;
      case RESOURCE:
        folder = Constants.EXPORT_RESOURCE_FOLDER;
        break;
      case FUNCTION:
        folder = Constants.EXPORT_FUNCTION_FOLDER;
        break;
      default:
        throw new MmaException("Unknown object type " + type
                                   + " in action " + id
                                   + ", restore config: " + GsonUtils.toJson(restoreConfig));
    }
    String prefix = OssUtils.getOssFolderToExportObject(
        restoreConfig.getBackupName(), folder, restoreConfig.getSourceDatabaseName());

    return OssUtils.listBucket(restoreConfig.getOssConfig(), prefix, delimiter);
  }

  @Override
  public String getName() { return this.id; }
}
