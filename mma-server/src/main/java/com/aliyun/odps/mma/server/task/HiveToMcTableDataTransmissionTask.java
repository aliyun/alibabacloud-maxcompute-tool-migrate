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

import java.util.List;

import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.OdpsConfig;
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
    ActionExecutionContext context = new ActionExecutionContext(config);
    OdpsConfig odpsConfig = (OdpsConfig) config.getDestDataConfig();
    HiveToMcTableDataTransmissionAction dataTransmissionAction =
        new HiveToMcTableDataTransmissionAction(
            id + ".DataTransmission",
            odpsConfig,
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_URL),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_USERNAME),
            config.get(JobConfiguration.DATA_SOURCE_HIVE_JDBC_PASSWORD),
            source,
            dest,
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
        odpsConfig,
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
