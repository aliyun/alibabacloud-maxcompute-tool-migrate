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
import java.util.Objects;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.action.*;
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
            List<Job> subJobs) throws MmaException {
        super(id, rootJobId, config, hiveTableMetaModel, mcTableMetaModel, job, subJobs);
        init();
    }

    private void init() throws MmaException {
        String executionProject = config.getOrDefault(
                JobConfiguration.JOB_EXECUTION_MC_PROJECT,
                config.get(JobConfiguration.DEST_CATALOG_NAME));
        ActionExecutionContext context = new ActionExecutionContext(config);

        String transmissionSettings = config.get(AbstractConfiguration.DATA_SOURCE_HIVE_TRANSMISSION_SETTINGS);
        boolean enableVerification = !Objects.equals(
                config.getOrDefault(AbstractConfiguration.DATA_ENABLE_VERIFICATION, "false"),
                "false"
        );

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
                        ConfigurationUtils.getSettingsMap(transmissionSettings),
                        this,
                        context);
        dag.addVertex(dataTransmissionAction);

        if (enableVerification) {
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

            HiveToOdpsTableHashVerificationAction verificationAction = new HiveToOdpsTableHashVerificationAction(
                    id + ".FinalVerification",
                    this,
                    context
            );
            dag.addVertex(verificationAction);

            dag.addEdge(dataTransmissionAction, mcVerificationAction);
            dag.addEdge(mcVerificationAction, verificationAction);
        }
    }

    @Override
    void updateMetadata() {
        job.setStatus(this);
    }
}
