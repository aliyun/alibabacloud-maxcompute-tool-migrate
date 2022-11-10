package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.mapper.JobMapper;
import com.aliyun.odps.mma.mapper.PartitionMapper;
import com.aliyun.odps.mma.mapper.TableMapper;
import com.aliyun.odps.mma.mapper.TaskMapper;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.service.TaskService;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.*;

@Component
public class TaskManager implements InitializingBean {
    private final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private TaskService taskService;
    private PartitionMapper ptMapper;
    SqlSessionFactory sqlSessionFactory;
    private ThreadPoolExecutor threadPool;
    private int maxTaskNum;
    private TaskUtils taskUtil;
    private final MMAConfig config;
    private final Map<Integer, Future<?>> taskFutures = new HashMap<>();
    private final Map<Integer, TaskExecutor> taskExecutors = new HashMap<>();
    private ApplicationEventPublisher publisher;

    public TaskManager(@Autowired MMAConfig config, ApplicationEventPublisher publisher) {
        this.config = config;
        this.publisher = publisher;
    }

    @Autowired
    public void setTaskService(TaskService taskService) {
        this.taskService = taskService;
    }

    @Autowired
    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    @Autowired
    public void setTaskUtil(TaskUtils taskUtil) {
        this.taskUtil = taskUtil;
    }

    @Autowired
    public void setPtMapper(PartitionMapper ptMapper) {
        this.ptMapper = ptMapper;
    }

    @Scheduled(fixedRateString = "${SCH_RATE:2000}", initialDelay = 2_000)
    public void executeTasks() {
        int activeTasks = this.threadPool.getActiveCount();
        int executorAvailable = this.maxTaskNum - activeTasks;

        if (executorAvailable <= 0) {
            return;
        }

        // select * from tasks where restart=1 or status=INIT order by restart desc, job_id limit n
        List<TaskModel> tasks = taskService.getTasksAvailable(executorAvailable);
        if (tasks.size() > 0) {
            logger.info("get available tasks num: {}", tasks.size());
        }

        for (TaskModel task: tasks) {
            if (taskFutures.containsKey(task.getId())) {
                continue;
            }

            TaskExecutor te = taskUtil.getTaskExecutor(task.getType());
            logger.info("submit task {}", task.getTaskName());
            te.setTask(task);

            CompletableFuture<?> cf = new CompletableFuture<>();
            Future<?> f = this.threadPool.submit(() -> {
                try {
                    te.run();
                } finally {
                    cf.complete(null);
                }

                return null;
            });

            taskFutures.put(task.getId(), f);
            taskExecutors.put(task.getId(), te);

            cf.thenAccept((Void) -> {
                logger.info("task is {} {}", task.getStatus(), task.getTaskName());
                taskFutures.remove(task.getId());
                taskExecutors.remove(task.getId());
            });
        }

        if (tasks.size() > 0) {
            logger.info("success submit tasks num: {}", tasks.size());
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        String taskMaxNumStr = System.getProperty("TASK_MAX_NUM");

        if (Objects.nonNull(taskMaxNumStr)) {
            this.maxTaskNum = Integer.parseInt(taskMaxNumStr);
            config.setConfig(MMAConfig.TASK_MAX_NUM, taskMaxNumStr);
        } else {
            this.maxTaskNum = config.getInteger(MMAConfig.TASK_MAX_NUM);;
        }

        logger.info("the max worker num is {}", this.maxTaskNum);
        threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(this.maxTaskNum);
        this.taskService.restartAllTerminated();
        this.ptMapper.setTerminatedPtStatusInit();
    }

    public void cancelTask(int taskId) {
        Future<?> future = taskFutures.get(taskId);
        TaskExecutor te = taskExecutors.get(taskId);
        if (Objects.isNull(future)) {
            return;
        }

        taskFutures.remove(taskId);
        taskExecutors.remove(taskId);
        if (! future.isDone()) {
            logger.info("try to stop task with id={}", taskId);
            te.killSelf();
            future.cancel(true);
        }

        taskService.setTaskStop(taskId);
    }

    public void cancelJob(int jobId) {
        logger.info("try to stop job {} ", jobId);
        stopTaskExecutor(jobId);
        setJobStopped(jobId);
        logger.info("success to stop job {} ", jobId);
    }

    public void deleteJob(int jobId) {
        logger.info("try to delete job {} ", jobId);
        stopTaskExecutor(jobId);
        setJobDeleted(jobId);
        logger.info("success to delete job {} ", jobId);
    }

    public void join() {
        while (this.threadPool.getActiveCount() != 0) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                logger.error("", e);
            }
        }

        this.threadPool.shutdown();
    }

    @Transactional
    public void setJobStopped(int jobId) {
        logger.info("start to stop job {}", jobId);
        // 设置job和task stopped=1, partition、非分区表状态设为init
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);
            TaskMapper taskMapper = sqlSession.getMapper(TaskMapper.class);
            PartitionMapper ptMapper = sqlSession.getMapper(PartitionMapper.class);
            TableMapper tableMapper = sqlSession.getMapper(TableMapper.class);

            jobMapper.setJobStop(jobId);
            taskMapper.setTasksStopByJobId(jobId);

            // 分区表把job对应的状态为DOING的分区状态设置为INIT
            ptMapper.setPartitionsStatusInitByJobId(jobId);

            // 非分区表把job对应的状态为DOING的Table的状态设置为INIT
            tableMapper.setNonPartitionedTableStatusInitByJobId(jobId);

            sqlSession.commit();
        }
        logger.info("success to stop job {}", jobId);
    }

    @Transactional
    public void setJobDeleted(int jobId) {
        // 设置job和task stopped=1, partition、非分区表状态设为init
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);
            TaskMapper taskMapper = sqlSession.getMapper(TaskMapper.class);
            PartitionMapper ptMapper = sqlSession.getMapper(PartitionMapper.class);
            TableMapper tableMapper = sqlSession.getMapper(TableMapper.class);

            jobMapper.setJobDeleted(jobId);
            taskMapper.setTasksDeletedByJobId(jobId);

            // 分区表把job对应的状态为DOING的分区状态设置为INIT
            ptMapper.setPartitionsStatusInitByJobId(jobId);

            // 非分区表把job对应的状态为DOING的Table的状态设置为INIT
            tableMapper.setNonPartitionedTableStatusInitByJobId(jobId);

            sqlSession.commit();
        }
    }

    private void stopTaskExecutor(int jobId) {
        List<TaskModel> tasks = taskService.getRunningTasksByJobId(jobId);

        // 停掉task
        for (TaskModel task: tasks) {
            task.setStopped(true); // 供task executor用
            publisher.publishEvent(new TaskEvent(this, task));
            int taskId = task.getId();
            Future<?> future = taskFutures.get(taskId);
            TaskExecutor te = taskExecutors.get(taskId);
            if (Objects.isNull(future)) {
                continue;
            }

            if (! future.isDone()) {
                logger.info("stop task with id={}", taskId);
                te.killSelf();
                future.cancel(true);
            }
        }

        // 在从taskFutures里去掉task
        for (TaskModel task: tasks) {
            int taskId = task.getId();
            taskFutures.remove(taskId);
            taskExecutors.remove(taskId);
        }
    }
}
