package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.mapper.JobMapper;
import com.aliyun.odps.mma.mapper.TaskMapper;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.service.JobService;
import com.aliyun.odps.mma.util.IdGen;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


import java.util.*;

@Service
@DependsOn("DBInitializer")
public class JobServiceImpl implements JobService, InitializingBean {
    Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

    @Autowired
    private JobMapper jobMapper;
    @Autowired
    private TaskMapper taskMapper;

    private IdGen jobIdGen;
    private IdGen taskIdGen;

    @Autowired
    SqlSessionFactory sqlSessionFactory;

    @Override
    public void insertJob(JobModel job) {
        jobMapper.insertJob(job);
    }

    @Override
    public JobModel getJobById(int id) {
        return jobMapper.getJobById(id);
    }

    @Override
    public List<JobModel> getJobs(JobFilter jobFilter) {
        return jobMapper.getJobs(jobFilter);
    }

    @Override
    public int getJobsCount(JobFilter jobFilter) {
        return jobMapper.getJobsCount(jobFilter);
    }

    @Override
    @Transactional
    public void submit(JobModel jobModel, List<TaskModel> tasks) {
        int jobId = jobIdGen.nextId();
        jobModel.setId(jobId);

        // 对task顺序进行重排，防止对同一个table的task挨在一起，这样可以避免
        // 对同一个table进行add partition的时候的锁竞争
        Map<String, List<TaskModel>> tableToTaskGroup = new HashMap<>();

        for (TaskModel taskModel: tasks) {
            String tableName = taskModel.getTableName();
            List<TaskModel> tasksOfTable = tableToTaskGroup.get(tableName);

            if (Objects.nonNull(tasksOfTable)) {
                tasksOfTable.add(taskModel);
            } else {
                tasksOfTable = new ArrayList<>();
                tasksOfTable.add(taskModel);

                tableToTaskGroup.put(tableName, tasksOfTable);
            }
        }

        List<TaskModel> tasksShuffled = new ArrayList<>();
        List<List<TaskModel>> taskGroups = new ArrayList<>(tableToTaskGroup.keySet().size());
        for (String tableName: tableToTaskGroup.keySet()) {
            taskGroups.add(tableToTaskGroup.get(tableName));
        }

        for (int i = 0; ; i ++) {
            boolean stop = true;

            for (List<TaskModel> taskGroup: taskGroups) {
                if (i < taskGroup.size()) {
                    tasksShuffled.add(taskGroup.get(i));
                    stop = false;
                }
            }

            if (stop) {
                break;
            }
        }

        assert tasks.size() == tasksShuffled.size();

        for (TaskModel taskModel: tasksShuffled) {
            taskModel.setId(taskIdGen.nextId());
            taskModel.setJobId(jobId);
        }

        logger.info("start to save {} tasks", tasks.size());

        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);
            TaskMapper taskMapper = sqlSession.getMapper(TaskMapper.class);

            jobMapper.insertJob(jobModel);
            for (TaskModel task: tasks) {
                taskMapper.insertTask(task);
            }

            for (TaskModel task: tasks) {
                List<Integer> partitions = task.getPartitions();

                if (Objects.nonNull(partitions)) {
                    for (Integer pid: partitions) {
                        taskMapper.insertTaskPartition(task.getJobId(), task.getId(), pid);
                    }
                }
            }

            sqlSession.commit();
        }

        logger.info("success to save {} tasks", tasks.size());
    }

    @Override
    public void setJobStop(int id) {
        this.jobMapper.setJobStop(id);
    }

    @Override
    public void retryJob(int jobId) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);

            jobMapper.resetJobStop(jobId);
            jobMapper.setFailedTasksOfJobRestart(jobId);

            sqlSession.commit();
        }
    }

    @Override
    public Map<Integer, String> getIdToDecOfJobs() {
        List<Map<String, Object>> jobs = this.jobMapper.getNameAndIdOfJobs();

        if (Objects.isNull(jobs)) {
            jobs = new ArrayList<>();
        }

        Map<Integer, String> idToDec = new HashMap<>();

        for (Map<String, Object> job: jobs) {
            int jobId = (int) job.get("id");
            String desc = (String) job.get("description");
            idToDec.put(jobId, desc);
        }

        return idToDec;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        jobIdGen = new IdGen(jobMapper.maxJobId());
        taskIdGen = new IdGen(taskMapper.maxTaskId());
    }
}
