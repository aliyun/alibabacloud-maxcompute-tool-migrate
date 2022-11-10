package com.aliyun.odps.mma.event;

import com.aliyun.odps.mma.constant.JobStatus;
import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.mapper.*;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.task.TaskEvent;
import lombok.Getter;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Component
public class TaskEventListener implements InitializingBean {
    private final TaskService taskService;
    private final SqlSessionFactory sqlSessionFactory;
    private final Set<TaskStatus> statusDoing;
    private final Set<TaskStatus> statusFailed;
    private final List<Integer> jobIds = new ArrayList<>();

    @Autowired
    public TaskEventListener(TaskService taskService, SqlSessionFactory sqlSessionFactory) {
        this.taskService = taskService;
        this.sqlSessionFactory = sqlSessionFactory;

        statusDoing = new HashSet<TaskStatus>() {{
            add(TaskStatus.SCHEMA_DOING);
            add(TaskStatus.SCHEMA_DONE);
            add(TaskStatus.DATA_DOING);
            add(TaskStatus.DATA_DONE);
            add(TaskStatus.VERIFICATION_DOING);
            add(TaskStatus.VERIFICATION_DONE);
        }};

        statusFailed = new HashSet<TaskStatus>() {{
            add(TaskStatus.SCHEMA_FAILED);
            add(TaskStatus.DATA_FAILED);
            add(TaskStatus.VERIFICATION_FAILED);
        }};
    }

    @EventListener
    @Async
    public synchronized void handleTaskEvent(TaskEvent taskEvent) {
        TaskModel task = taskEvent.getTaskModel();
        jobIds.add(task.getJobId());
    }

    @Scheduled(initialDelay = 3_000, fixedDelay = 3_000)
    public void run() {
        List<TaskModel> tasks;

        synchronized (this) {
            tasks = taskService.getTaskStatusByJobIds(this.jobIds);
            this.jobIds.clear();
        }

        updateStatus(tasks);
    }

    @Transactional
    void updateStatus(List<TaskModel> tasks) {
        if (tasks.size() == 0) {
            return;
        }

        StatusGroup jobStatusGroup = getStatusGroup(tasks, TaskModel::getJobId);

        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            JobMapper jobMapper = sqlSession.getMapper(JobMapper.class);

            if (jobStatusGroup.getFailed().size() > 0) {
                jobMapper.updateJobsStatus(JobStatus.FAILED, jobStatusGroup.getFailed());

            } else if (jobStatusGroup.getDoing().size() > 0) {
                jobMapper.updateJobsStatus(JobStatus.DOING, jobStatusGroup.getDoing());
            }

            if (jobStatusGroup.getDone().size() > 0) {
                jobMapper.updateJobsStatus(JobStatus.DONE, jobStatusGroup.getDone());
            }

            sqlSession.commit();
        }
    }

    private StatusGroup getStatusGroup(List<TaskModel> tasks, IdGetFunc func) {
        HashMap<Integer, List<TaskStatus>> idToStatusList = new HashMap<>();

        for(TaskModel task: tasks) {
            Integer targetId = func.call(task);
            List<TaskStatus> statusList = idToStatusList.get(targetId);

            if (Objects.isNull(statusList)) {
                statusList = new ArrayList<>();
                idToStatusList.put(targetId, statusList);
            }

            if (! task.isStopped()) {
                statusList.add(task.getStatus());
            } else {
                statusList.add(TaskStatus.INIT);
            }
        }

        StatusGroup statusGroup = new StatusGroup();

        for (Integer targetId: idToStatusList.keySet()) {
            List<TaskStatus> statusList = idToStatusList.get(targetId);

            if (statusList.stream().anyMatch(statusDoing::contains)) {
                statusGroup.addDoing(targetId);
                continue;
            }

            if (statusList.stream().anyMatch(statusFailed::contains)) {
                statusGroup.addFailed(targetId);
                continue;
            }

            if (statusList.stream().anyMatch(s -> s == TaskStatus.INIT)) {
                statusGroup.addStopped(targetId);
                continue;
            }

            statusGroup.addDone(targetId);
        }

        return statusGroup;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        List<TaskModel> taskModels = taskService.getTaskStatusByJobIds(Collections.emptyList());
        updateStatus(taskModels);
    }

    @Getter
    private static class StatusGroup {
        List<Integer> doing= new ArrayList<>();
        List<Integer> failed = new ArrayList<>();
        List<Integer> done = new ArrayList<>();
        List<Integer> stopped = new ArrayList<>();

        public void addDoing(Integer id) {
            this.doing.add(id);
        }

        public void addFailed(Integer id) {
            this.failed.add(id);
        }

        public void addDone(Integer id) {
            this.done.add(id);
        }

        public void addStopped(Integer id) { this.stopped.add(id); }
    }

    @FunctionalInterface
    private interface IdGetFunc {
        Integer call(TaskModel task);
    }
}