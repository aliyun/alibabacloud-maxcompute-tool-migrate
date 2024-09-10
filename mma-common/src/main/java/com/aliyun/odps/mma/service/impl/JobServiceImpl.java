package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.constant.JobBatchStatus;
import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.jdbc.IntSetter;
import com.aliyun.odps.mma.jdbc.Setter;
import com.aliyun.odps.mma.jdbc.SqlUtils;
import com.aliyun.odps.mma.mapper.JobDao;
import com.aliyun.odps.mma.mapper.JobMapper;
import com.aliyun.odps.mma.mapper.TaskMapper;
import com.aliyun.odps.mma.model.JobBatchModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.service.JobService;
import com.aliyun.odps.mma.util.MysqlConfig;
import com.aliyun.odps.mma.util.id.IdGenException;
import com.aliyun.odps.mma.util.id.JobIdGen;
import com.aliyun.odps.mma.util.id.TaskIdGen;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Service
@DependsOn("DBInitializer")
public class JobServiceImpl implements JobService {
    private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);
    @Autowired
    private JobMapper jobMapper;
    @Autowired
    private TaskMapper taskMapper;
    @Autowired
    private JobIdGen jobIdGen;
    @Autowired
    private TaskIdGen taskIdGen;
    @Autowired
    private SqlSessionFactory sqlSessionFactory;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private MysqlConfig mysqlConfig;

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
    public void submit(JobModel jobModel, List<TaskModel> tasks) {
        // 定时任务可能会产生空的子任务列表
        if (tasks.isEmpty()) {
            return;
        }

        try {
            Integer jobId = jobModel.getId();
            Integer jobBatchId = 0;
            boolean jobIsNew= Objects.isNull(jobId) || jobId == 0;

            if (jobIsNew) {
                jobId = jobIdGen.nextId();
                jobModel.setId(jobId);
            } else {
                jobBatchId = nextJobBatchId(jobId);
            }

            // 构建新的批次
            JobBatchModel jobBatch = JobBatchModel
                    .builder()
                    .jobId(jobId)
                    .batchId(jobBatchId)
                    .status(JobBatchStatus.OK)
                    .build();

            // 对task顺序进行重排，防止对同一个table的task挨在一起，这样可以避免
            // 对同一个table进行add partition的时候的锁竞争
            List<TaskModel> tasksShuffled = shuffleTasks(tasks);
            for (TaskModel taskModel: tasksShuffled) {
                taskModel.setId(taskIdGen.nextId());
                taskModel.setJobId(jobId);
                taskModel.setBatchId(jobBatchId);
            }

            logger.info("start to save {} tasks", tasks.size());

            _submit(jobIsNew, jobModel, jobBatch, tasks);
        } catch (IdGenException e) {
            throw new RuntimeException(e);
        }

        logger.info("success to save {} tasks", tasks.size());
    }

    public void _submit(boolean jobIsNew, JobModel jobModel, JobBatchModel jobBatch, List<TaskModel> tasks) {
        if (Objects.nonNull(jobModel.getDeleted()) && jobModel.getDeleted()) {
            // 有定时任务的场景下，可能存在job已经删除，但是恰好定时任务要提交新的子任务的情况
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            SqlUtils.startTransaction(conn);
            // 先分批提交tasks，task_partitions
            JobDao.saveTaskAndPartitions(conn, tasks);
            JobDao.saveJob(conn, jobIsNew, jobModel, jobBatch);
            conn.commit();
        } catch (Throwable e) {
            logger.error("submit failed", e);
        }
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
    public void startJob(int jobId) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);

            jobMapper.resetJobStop(jobId);
            jobMapper.setTerminatedTasksOfJobRestart(jobId);

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
    public void insertJobBatch(JobBatchModel jobBatch) {
        try {
            int batchId = nextJobBatchId(jobBatch.getJobId());
            jobBatch.setBatchId(batchId);

            try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
                jobMapper.insertJobBatch(jobBatch);
                jobMapper.updateJobBatchId(jobBatch.getJobId(), jobBatch.getBatchId());

                sqlSession.commit();
            }

        } catch (IdGenException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<JobBatchModel> listJobBatches(int jobId) {
        return jobMapper.listJobBatches(jobId);
    }

    @Override
    public List<JobModel> listJobsWithTimer() {
        return jobMapper.listJobsWithTimer();
    }

    private List<TaskModel> shuffleTasks(List<TaskModel> tasks) {
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
        return tasksShuffled;
    }

    private int nextJobBatchId(int jobId) throws IdGenException {
        try(Connection conn = dataSource.getConnection()) {
            try {
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                try (PreparedStatement stat1 = conn.prepareStatement("update job set last_batch = last_batch + 1 where id=(?)")) {
                    stat1.setInt(1, jobId);
                    stat1.execute();
                }

                try (PreparedStatement stat2 = conn.prepareStatement("select last_batch from job where id=(?)")) {
                    stat2.setInt(1, jobId);
                    ResultSet rs = stat2.executeQuery();
                    rs.next();
                    int id = rs.getInt(1);
                    conn.commit();
                    return id;
                }
            } catch (SQLException e) {
                conn.rollback();
                throw new IdGenException(e);
            } finally {
                conn.setAutoCommit(true);
            }

        } catch (SQLException e) {
            throw new IdGenException(e);
        }
    }
}
