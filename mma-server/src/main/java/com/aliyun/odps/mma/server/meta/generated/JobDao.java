package com.aliyun.odps.mma.server.meta.generated;

import java.util.List;

import org.apache.ibatis.annotations.Param;

public interface JobDao {

  int deleteJobById(String jobId);

  int deleteSubJobById(
      @Param("parentJobId") String parentJobId,
      @Param("subJobId") String subJobId);

  int insertJob(Job record);

  int insertSubJob(
      @Param("parentJobId") String parentJobId,
      @Param("record") Job record);

//    int insertSelective(Job record);

  Job selectJobById(String jobId);

  Job selectSubJobById(
      @Param("parentJobId") String parentJobId,
      @Param("subJobId") String subJobId);

  List<Job> selectJobs();

  List<Job> selectSubJobs(@Param("parentJobId") String parentJobId);

  List<Job> selectJobsByJobStatus(String jobStatus);

  List<Job> selectSubJobsByJobStatus(
      @Param("parentJobId") String parentJobId,
      @Param("jobStatus") String jobStatus);

  int updateJobByPrimaryKeySelective(Job record);

  int updateJobById(Job record);

  int updateSubJobById(
      @Param("parentJobId") String parentJobId,
      @Param("record") Job record);

  void createMmaSchemaIfNotExists();

  void createJobTableIfNotExists();

  void createSubJobTableIfNotExists(String parentJobId);

  void dropSubJobTable(String parentJobId);
}