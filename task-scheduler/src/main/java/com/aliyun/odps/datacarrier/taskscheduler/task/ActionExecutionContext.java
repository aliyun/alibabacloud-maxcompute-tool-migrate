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

package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.List;
import java.util.Objects;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;


public class ActionExecutionContext
{
  private String jobId;
  private MetaSource.TableMetaModel tableMetaModel;
  private List<List<String>> sourceVerificationResult;
  private List<List<String>> destVerificationResult;
  private MmaConfig.OdpsConfig odpsConfig;
  private MmaConfig.OssConfig ossConfig;

  public ActionExecutionContext(String jobId) {
    this.tableMetaModel = null;
    this.sourceVerificationResult = null;
    this.destVerificationResult = null;
    this.odpsConfig = null;
    this.ossConfig = null;

    this.jobId = Objects.requireNonNull(jobId);
  }

  public MetaSource.TableMetaModel getTableMetaModel() {
    return tableMetaModel;
  }

  public String getJobId() {
    return jobId;
  }
  public MmaConfig.OdpsConfig getOdpsConfig() {
    return odpsConfig;
  }

  public void setOdpsConfig(MmaConfig.OdpsConfig odpsConfig) {
    this.odpsConfig = odpsConfig;
  }

  public MmaConfig.OssConfig getOssConfig() {
    return ossConfig;
  }

  public void setOssConfig(MmaConfig.OssConfig ossConfig) {
    this.ossConfig = ossConfig;
  }

  public void setTableMetaModel(MetaSource.TableMetaModel tableMetaModel) {
    this.tableMetaModel = tableMetaModel;
  }

  public List<List<String>> getSourceVerificationResult() {
    return sourceVerificationResult;
  }

  public void setSourceVerificationResult(List<List<String>> rows) {
    sourceVerificationResult = rows;
  }

  public List<List<String>> getDestVerificationResult() {
    return destVerificationResult;
  }

  public void setDestVerificationResult(List<List<String>> rows) {
    destVerificationResult = rows;
  }
}
