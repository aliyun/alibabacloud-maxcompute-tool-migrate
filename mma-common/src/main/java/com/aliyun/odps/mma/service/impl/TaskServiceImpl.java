package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.mapper.TableMapper;
import com.aliyun.odps.mma.mapper.TaskMapper;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.model.TaskLog;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.query.TaskFilter;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.util.StepIter;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Service
public class TaskServiceImpl implements TaskService {
    @Autowired
    TaskMapper taskMapper;

    @Autowired
    TableMapper tableMapper;

    @Autowired
    SqlSessionFactory sqlSessionFactory;

    @Override
    public void insertTask(TaskModel task) {
        taskMapper.insertTask(task);
    }

    @Override
    public TaskModel getTaskById(int id) {
        return taskMapper.getTaskById(id);
    }

    @Override
    public List<TaskModel> getTasks(TaskFilter taskFilter) {
        return taskMapper.getTasks(taskFilter);
    }

    @Override
    public int getTasksCount(TaskFilter taskFilter) {
        return taskMapper.getTasksCount(taskFilter);
    }

    @Override
    public void addTaskLog(TaskLog taskLog) {
        taskMapper.addTaskLog(taskLog);
    }

    @Override
    public void updateTaskStatus(TaskModel task) {
        taskMapper.updateTaskStatus(task);
    }

    @Override
    public void updateTaskSubStatus(TaskModel task) {
        taskMapper.updateTaskSubStatus(task);
    }

    @Override
    public void updateTaskStatus(int taskId, TaskStatus taskStatus) {
        TaskModel taskModel = new TaskModel();
        taskModel.setId(taskId);
        taskModel.setStatus(taskStatus);
        updateTaskStatus(taskModel);
    }

    @Override
    public void setTaskStart(int taskId) {
        this.taskMapper.setTaskStart(taskId);
    }

    @Override
    public void setTaskEnd(int taskId) {
        this.taskMapper.setTaskEnd(taskId);
    }

    @Override
    public void setTaskStop(int taskId) {
        this.taskMapper.setTaskStop(taskId);
    }

    @Override
    public void batchSetTaskStop(List<Integer> taskIds) {
        this.taskMapper.batchSetTaskStop(taskIds);
    }

    @Override
    public void resetRestart(int id) {
        taskMapper.resetRestart(id);
    }

    @Override
    public void restart(int id) {
        taskMapper.restart(id);
    }

    @Override
    public void reset(int id) {
        taskMapper.reset(id);
    }

    @Override
    public void restartAllTerminated() {
        taskMapper.restartAllTerminated();
    }

    @Override
    public List<TaskLog> getTaskLogs(int taskId) {
        return taskMapper.getTaskLogs(taskId);
    }

    @Override
    public TableModel getTableOfTask(int tableId) {
        return tableMapper.getTableById(tableId);
    }

    @Override
    public List<TaskModel> getTasksAvailable(int limit) {
        return taskMapper.getTasksAvailable(limit);
    }

    @Override
    public List<TaskModel> getRunningTasks(List<Integer> partitionIds, List<Integer> tableIds) {
        boolean ptIdsIsEmpty = Objects.isNull(partitionIds) || partitionIds.size() == 0;
        boolean tableIdsIsEmpty =  Objects.isNull(tableIds) || tableIds.size() == 0;

        if (ptIdsIsEmpty && tableIdsIsEmpty) {
            return Collections.emptyList();
        }

        StepIter<Integer> ptStepIter = new StepIter<>(partitionIds, 10000);
        List<TaskModel> tasks = new ArrayList<>();

        if (! tableIdsIsEmpty) {
            List<TaskModel> taskSubList = taskMapper.getRunningTasksByTableIds(tableIds);
            if (Objects.nonNull(taskSubList)) {
                tasks.addAll(taskSubList);
            }
        }


        for (List<Integer> ptSubList: ptStepIter) {
            List<TaskModel> taskSubList = taskMapper.getRunningTasksByPtIds(ptSubList);
            if (Objects.nonNull(taskSubList)) {
                tasks.addAll(taskSubList);
            }
        }

        return tasks;
    }

    @Override
    public List<TaskModel> getRunningTasksByJobId(int jobId) {
        return this.taskMapper.getRunningTasksByJobId(jobId);
    }

    @Override
    public List<TaskModel> getTaskStatusByJobIds(List<Integer> jobIds) {
        if (jobIds.size() == 0) {
            return new ArrayList<>();
        }

        List<TaskModel> tasks = taskMapper.getTaskStatusByJobIds(jobIds);
        if (Objects.isNull(tasks)) {
            return new ArrayList<>();
        }

        return tasks;
    }

    @Override
    public int getPartitionNumOfTask(int taskId) {
        return taskMapper.getPartitionNumOfTask(taskId);
    }

    @Override
    public List<Integer> getJobIdsByJobFilter(JobFilter jobFilter) {
        if (jobFilter.hasNoConditionsForTask()) {
            return  new ArrayList<>();
        }

        return taskMapper.getJobIdsByJobFilter(jobFilter);
    }

    @Override
    public List<Integer> getTaskIdsByPt(String tableName, String ptValue) {
        List<Integer> taskIds = taskMapper.getTaskIdsByPt(tableName, ptValue);

        if (Objects.isNull(taskIds)) {
            return new ArrayList<>();
        }

        return taskIds;
    }
}
