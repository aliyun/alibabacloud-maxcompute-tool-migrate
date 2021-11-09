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

package com.aliyun.odps.mma.server.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.server.action.ActionExecutionContext;
import com.aliyun.odps.mma.server.action.HiveToMcTableDataTransmissionAction;
import com.aliyun.odps.mma.server.action.HiveVerificationAction;
import com.aliyun.odps.mma.server.action.McVerificationAction;
import com.aliyun.odps.mma.server.action.VerificationAction;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

public class HiveToMcTableDataTransmissionTask extends TableDataTransmissionTask {

  public HiveToMcTableDataTransmissionTask(
      String id,
      String rootJobId,
      JobConfiguration config,
      TableMetaModel hiveTableMetaModel,
      TableMetaModel mcTableMetaModel,
      Job job,
      List<Job> subJobs) {
    super(id, rootJobId, config, hiveTableMetaModel, mcTableMetaModel, job, subJobs);
    init();
  }

  private void init() {
    String executionProject = config.getOrDefault(
        JobConfiguration.JOB_EXECUTION_MC_PROJECT,
        config.get(JobConfiguration.DEST_CATALOG_NAME));
    ActionExecutionContext context = new ActionExecutionContext(config);

    String userHiveConfig = config.get(AbstractConfiguration.DATA_SOURCE_HIVE_RUNTIME_CONFIG);
    Map<String, String> userHiveConfigMap = new HashMap<>();
    for (String s : userHiveConfig.split(";")) {
      String[] kv = s.split("=");
      userHiveConfigMap.put(kv[0].trim(), kv[1].trim());
    }

    HiveToMcTableDataTransmissionAction dataTransmissionAction =
        new HiveToMcTableDataTransmissionAction(
            id + ".DataTransmission",
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
            config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
            executionProject,
            config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_URL),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_USERNAME),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_PASSWORD),
            source,
            dest,
            userHiveConfigMap,
            this,
            context);
    dag.addVertex(dataTransmissionAction);

    HiveVerificationAction hiveVerificationAction = new HiveVerificationAction(
        id + ".HiveDataVerification",
        config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_URL),
        config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_USERNAME),
        config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_PASSWORD),
        source,
        true,
        this,
        context);
    dag.addVertex(hiveVerificationAction);

    McVerificationAction mcVerificationAction = new McVerificationAction(
        id + ".McDataVerification",
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID),
        config.get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET),
        executionProject,
        config.get(JobConfiguration.DATA_DEST_MC_ENDPOINT),
        dest,
        false,
        this,
        context);
    dag.addVertex(mcVerificationAction);

    VerificationAction verificationAction = new VerificationAction(
        id + ".FinalVerification",
        source,
        this,
        context);
    dag.addVertex(verificationAction);

    dag.addEdge(dataTransmissionAction, hiveVerificationAction);
    dag.addEdge(dataTransmissionAction, mcVerificationAction);
    dag.addEdge(hiveVerificationAction, verificationAction);
    dag.addEdge(mcVerificationAction, verificationAction);
  }

  @Override
  void updateMetadata() {
    job.setStatus(this);
  }
}
