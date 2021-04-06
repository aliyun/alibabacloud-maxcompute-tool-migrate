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

package com.aliyun.odps.mma.config;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class JobInfoOutputsV1 {
  @Expose
  @SerializedName("JobId")
  String jobId;

  @Expose
  @SerializedName("ObjectType")
  String objectType;

  @Expose
  @SerializedName("SourceCatalog")
  String sourceCatalog;

  @Expose
  @SerializedName("SourceObject")
  String sourceObject;

  @Expose
  @SerializedName("DestCatalog")
  String destCatalog;

  @Expose
  @SerializedName("DestObject")
  String destObject;

  @Expose
  @SerializedName("Status")
  String status;

  @Expose
  @SerializedName("Progress")
  Double progress;

  public JobInfoOutputsV1(
      String jobId,
      String objectType,
      String sourceCatalog,
      String sourceObject,
      String destCatalog,
      String destObject,
      String status,
      Double progress) {
    this.jobId = jobId;
    this.objectType = objectType;
    this.sourceCatalog = sourceCatalog;
    this.sourceObject = sourceObject;
    this.destCatalog = destCatalog;
    this.destObject = destObject;
    this.status = status;
    this.progress = progress;
  }

  public String getJobId() {
    return this.jobId;
  }

  public String getObjectType() {
    return objectType;
  }

  public String getSourceCatalog() {
    return sourceCatalog;
  }

  public String getSourceObject() {
    return sourceObject;
  }

  public String getDestCatalog() {
    return destCatalog;
  }

  public String getDestObject() {
    return destObject;
  }

  public String getStatus() {
    return this.status;
  }

  public Double getProgress() {
    return this.progress;
  }
}
