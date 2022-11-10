package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.constant.JobStatus;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.query.JobFilter;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public interface JobMapper {
    void insertJob(JobModel job);
    JobModel getJobById(@Param("id") int id);
    List<JobModel> getJobs(JobFilter jobFilter);
    int getJobsCount(JobFilter jobFilter);
    Integer maxJobId();
    void updateJobsStatus(@Param("status") JobStatus status, @Param("ids") List<Integer> ids);
    void setJobStop(@Param("id") int id);
    void resetJobStop(@Param("id") int id);
    void setJobRestart(@Param("id") int id);
    void setFailedTasksOfJobRestart(@Param("jobId") int jobId);
    void setJobDeleted(@Param("id") int id);
    List<Map<String, Object>> getNameAndIdOfJobs();
}
