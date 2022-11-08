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

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class JobInfoOutputsV1<T1,T2> extends Object {
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
  @SerializedName("Priority")
  int Priority;
  
  @Expose
  @SerializedName("createTime")
  Long createTime;
  
  @Expose
  @SerializedName("startTime")
  Long startTime;
  
  @Expose
  @SerializedName("endTime")
  Long endTime;

  @Expose
  @SerializedName("Status")
  String status;

  @Expose
  @SerializedName("Progress")
  Double progress;
  
  @Expose
  @SerializedName("SubJobs")
  T1 SubJobs;
  
  @Expose
  @SerializedName("Tasks")
  T2 Tasks;

  public JobInfoOutputsV1(
      String jobId,
      String objectType,
      String sourceCatalog,
      String sourceObject,
      String destCatalog,
      String destObject,
      int Priority,
      Long createTime,
      Long startTime,
      Long endTime,
      String status,
      Double progress,
      T1 SubJobs,
      T2 Tasks) {
    this.jobId = jobId;
    this.objectType = objectType;
    this.sourceCatalog = sourceCatalog;
    this.sourceObject = sourceObject;
    this.destCatalog = destCatalog;
    this.destObject = destObject;
    this.Priority = Priority;
    this.createTime = createTime;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
    this.progress = progress;
    this.SubJobs=SubJobs;
    this.Tasks=Tasks;
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
  
  public int getPriority() {
	    return Priority;
  }
  
  public Long getcreateTime() {
	    return createTime;
  }

  public Long getstartTime() {
	    return startTime;
  }
  
  public Long getendTime() {
	    return endTime;
  }
  
  public String getStatus() {
    return this.status;
  }

  public Double getProgress() {
    return this.progress;
  }
  public T1 getSubJobs() {
	    return this.SubJobs;
  }
  
  public T2 getTasks() {
	    return this.Tasks;
  }
}

