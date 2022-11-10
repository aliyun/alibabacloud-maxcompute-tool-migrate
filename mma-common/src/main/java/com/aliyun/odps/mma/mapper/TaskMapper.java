package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.model.TaskLog;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.query.TaskFilter;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface TaskMapper {
    void insertTask(TaskModel task);
    TaskModel getTaskById(@Param("id") int id);
    List<TaskModel> getTasks(TaskFilter taskFilter);
    int getTasksCount(TaskFilter taskFilter);
    void addTaskLog(TaskLog taskLog);
    void updateTaskStatus(TaskModel task);
    void setTaskStart(@Param("id") int taskId);
    void setTaskEnd(@Param("id") int taskId);
    void setTaskStop(@Param("id") int taskId);
    void batchSetTaskStop(@Param("ids") List<Integer> taskIds);
    void resetRestart(@Param("id") int id);
    void restart(@Param("id") int id);
    void restartAllTerminated();
    List<TaskLog> getTaskLogs(@Param("taskId") int taskId);
    void insertTaskPartition(@Param("jobId") Integer jobId, @Param("taskId") Integer taskId, @Param("partitionId") Integer partitionId);
    Integer maxTaskId();
    List<TaskModel> getTasksAvailable(@Param("limit") int limit);
    List<TaskModel> getRunningTasksByTableIds(@Param("tableIds") List<Integer> tableIds);
    List<TaskModel> getRunningTasksByPtIds(@Param("partitionIds") List<Integer> partitionIds);
    List<TaskModel> getRunningTasksByJobId(@Param("jobId") int jobId);
    List<TaskModel> getTaskStatusByJobIds(@Param("jobIds") List<Integer> jobIds);
    void setTasksStopByJobId(@Param("jobId") int jobId);
    void setTasksDeletedByJobId(@Param("jobId") int jobId);
    int getPartitionNumOfTask(@Param("taskId") int taskId);
    List<Integer> getJobIdsByJobFilter(JobFilter jobFilter);
    List<Integer> getTaskIdsByPt(@Param("tableName") String tableName, @Param("ptValue") String ptValue);
}
