package com.aliyun.odps.mma.service;

import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.model.TaskLog;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.query.TaskFilter;
import org.apache.ibatis.annotations.Param;

import java.util.Collections;
import java.util.List;

public interface TaskService {
    void insertTask(TaskModel task);
    TaskModel getTaskById(int taskId);
    List<TaskModel> getTasks(TaskFilter taskFilter);
    int getTasksCount(TaskFilter taskFilter);
    void addTaskLog(TaskLog taskLog);
    void updateTaskStatus(TaskModel task);
    void updateTaskSubStatus(TaskModel task);
    void updateTaskStatus(int taskId, TaskStatus taskStatus);
    void setTaskStart(int taskId);
    void setTaskEnd(int taskId);
    void setTaskStop(int taskId);
    void batchSetTaskStop(List<Integer> taskIds);
    void resetRestart(int id);
    void restart(int id);
    void reset(int id);
    void restartAllTerminated();
    List<TaskLog> getTaskLogs(int id);
    TableModel getTableOfTask(int tableId);
    List<TaskModel> getTasksAvailable(int limit);
    List<TaskModel> getRunningTasks(List<Integer> partitionIds, List<Integer> tableIds);
    List<TaskModel> getRunningTasksByJobId(@Param("jobId") int jobId);
    default List<TaskModel> getRunningTasks(List<Integer> partitionIds) {
        return getRunningTasks(partitionIds, Collections.emptyList());
    }
    List<TaskModel> getTaskStatusByJobIds(List<Integer> jobIds);
    int getPartitionNumOfTask(int taskId);
    List<Integer> getJobIdsByJobFilter(JobFilter jobFilter);
    List<Integer> getTaskIdsByPt(String tableName, String ptValue);
}
