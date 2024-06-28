package com.aliyun.odps.mma.service;

import com.aliyun.odps.mma.model.JobBatchModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.query.JobFilter;

import java.util.List;
import java.util.Map;

public interface JobService {
    void insertJob(JobModel job);
    JobModel getJobById(int id);
    List<JobModel> getJobs(JobFilter jobFilter);
    int getJobsCount(JobFilter jobFilter);
    void submit(JobModel model, List<TaskModel> tasks);
    void setJobStop(int id);
    void retryJob(int jobId);
    Map<Integer, String> getIdToDecOfJobs();
    void insertJobBatch(JobBatchModel jobBatch);

    List<JobBatchModel> listJobBatches(int jobId);
    List<JobModel> listJobsWithTimer();

}
