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

package com.aliyun.odps.mma.server.meta.generated;

import java.util.List;

import org.apache.ibatis.annotations.Param;

public interface JobDao {

  int deleteJobById(String jobId);

  int deleteSubJobById(
      @Param("parentJobId") String parentJobId,
      @Param("subJobId") String subJobId);

  int insertJob(JobRecord record);

  int insertSubJob(
      @Param("parentJobId") String parentJobId,
      @Param("record") JobRecord record);

//    int insertSelective(Job record);

  JobRecord selectJobById(String jobId);

  JobRecord selectSubJobById(
      @Param("parentJobId") String parentJobId,
      @Param("subJobId") String subJobId);

  List<JobRecord> selectJobs();

  List<JobRecord> selectSubJobs(@Param("parentJobId") String parentJobId);

  List<JobRecord> selectJobsByJobStatus(String jobStatus);

  List<JobRecord> selectSubJobsByJobStatus(
      @Param("parentJobId") String parentJobId,
      @Param("jobStatus") String jobStatus);

  int updateJobByPrimaryKeySelective(JobRecord record);

  int updateJobById(JobRecord record);

  int updateSubJobById(
      @Param("parentJobId") String parentJobId,
      @Param("record") JobRecord record);

  void createMmaSchemaIfNotExists();

  void createJobTableIfNotExists();

  void createSubJobTableIfNotExists(String parentJobId);

  void dropSubJobTable(String parentJobId);
}