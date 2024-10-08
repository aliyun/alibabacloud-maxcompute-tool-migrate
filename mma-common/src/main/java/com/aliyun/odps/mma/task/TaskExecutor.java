package com.aliyun.odps.mma.task;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.execption.VerificationFailed;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.orm.TableProxy;
import com.aliyun.odps.mma.util.ExceptionUtils;
import com.aliyun.odps.mma.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.meta.schema.OdpsSchemaAdapter;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.service.PartitionService;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.service.TaskService;

import org.springframework.context.ApplicationEventPublisher;

import lombok.AllArgsConstructor;


@AllArgsConstructor
public class TaskExecutor implements Runnable, TaskExecutorInter {
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    protected TableService tableService;
    protected TaskService taskService;
    protected PartitionService partitionService;
    protected OdpsSchemaAdapter schemaAdapter;

    protected MMAConfig mmaConfig;
    protected OrmFactory proxyFactory;
    protected OdpsAction odpsAction;
    protected TaskModel taskModel;
    protected TaskProxy task;
    protected JobModel jobModel;
    protected JobConfig jobConfig;
    protected SourceConfig sourceConfig;

    protected Odps odps;
    protected Table odpsTable;
    protected ApplicationEventPublisher publisher;

    protected boolean stopped = false;
    protected int partitionNumOfTask;

    public TaskExecutor() {}

    @Autowired
    public void init(
            TaskService taskService,
            TableService tableService,
            PartitionService partitionService,
            OrmFactory proxyFactory,
            MMAConfig config,
            ApplicationEventPublisher publisher
    ) {
        this.tableService = tableService;
        this.taskService = taskService;
        this.partitionService = partitionService;
        this.proxyFactory = proxyFactory;
        this.mmaConfig = config;
        this.publisher = publisher;
    }

    protected void setUp() {

    }

    public void setTask(TaskModel taskModel) {
        this.taskModel = taskModel;
    }

    public void _initTask() {
        this.task = proxyFactory.newTaskProxy(taskModel);
        jobModel = task.getJobModel();
        jobConfig = task.getJobConfig();
        sourceConfig = jobConfig.getSourceConfig();
        OdpsUtils odpsUtils = OdpsUtils.fromConfig(mmaConfig);
        this.odpsAction = new OdpsAction(odpsUtils, this.task);
        this.odps = odpsUtils.getOdps();

        partitionNumOfTask = task.getPartitions().size();
    }

    @Override
    public void run() {
        try {
            _initTask();
            setUp();

            if (task.isRestarted()) {
                task.resetRestart();
            }

            publishTaskEvent();
            task.setTaskStart();
            updateMigrationTargetStatus(MigrationStatus.DOING);
            while (task.getStatus() != TaskStatus.DONE && !stopped) {
                if (task.getStatus() == TaskStatus.SCHEMA_DONE) {
                    TableProxy table = task.getTable();
                    // 分区表无分区的情况建完task后直接结束任务
                    if (table.isPartitionedTable() && partitionNumOfTask == 0) {
                        publishTaskEvent();
                        updateMigrationTargetStatus(MigrationStatus.DONE);
                        task.setTaskEnd();
                        return;
                    }
                }

                switch (task.getStatus()) {
                    case INIT:
                    case SCHEMA_DOING:
                    case SCHEMA_FAILED:
                        setUpSchema();
                        if (task.getStatus() == TaskStatus.SCHEMA_FAILED) {
                            return;
                        }
                        if (jobConfig.getSchemaOnly()) {
                            logger.info("{} is wanted to create schema only", task.getTaskName());
                            publishTaskEvent();
                            updateMigrationTargetStatus(MigrationStatus.DONE);
                            task.setTaskEnd();
                        }
                        break;

                    case SCHEMA_DONE:
                    case DATA_DOING:
                    case DATA_FAILED:
                    // 校验失败，重新回到数据迁移阶段
                    case VERIFICATION_FAILED:
                        if (task.getStatus() == TaskStatus.VERIFICATION_FAILED) {
                            task.setSubStatus("");
                        }

                        dataTruncate();
                        if (task.getStatus() == TaskStatus.DATA_FAILED) {
                            return;
                        }

                        dataTrans();

                        if (task.getStatus() == TaskStatus.DATA_FAILED) {
                            return;
                        }
                        break;

                    case DATA_DONE:
                    case VERIFICATION_DOING:
                        if (! jobConfig.isEnableVerification()) {
                            task.setStatus(TaskStatus.VERIFICATION_DONE);
                            break;
                        }

                        verifyData();

                        if (task.getStatus() == TaskStatus.VERIFICATION_FAILED) {
                            return;
                        }

                        break;
                    case VERIFICATION_DONE:
                        publishTaskEvent();
                        updateMigrationTargetStatus(MigrationStatus.DONE);
                        logger.info("task {} is done", task.getTaskName());
                        task.setTaskEnd();
                        task.log("done", "task is success");
                        return;

                    default:
                        throw new RuntimeException("Unsupported task status: " + task.getStatus());
                }
            }
        } catch (Exception e) {
            publishTaskEvent();

            task.error("unexpected error", ExceptionUtils.getStackTrace(e));
            logger.error("unexpected error", e);
        }

        publishTaskEvent();
    }

    protected void withStatus(TaskStatus startStatus, TaskStatus endStatus, TaskStatus errStatus, ActionFunc func) {
        task.setStatus(startStatus);
        try {
            func.run();

//            if (task.getStatus() == errStatus) {
//                throw new MMATaskInterruptException();
//            }
        } catch (Exception e) {
            if (this.stopped) {
                logger.info("task {} is stopped by kill self", task.getTaskName());
                task.log("stop", "be stopped");
                return;
            }

            task.setStatus(errStatus);

            updateMigrationTargetStatus(MigrationStatus.FAILED);
            publishTaskEvent();

            if (e instanceof InterruptedException) {
                logger.info("task {} is stopped by interruption", task.getTaskName());
                task.log("stop", "be stopped");
                stopped = true;
                return;
            }

            if (! (e instanceof MMATaskInterruptException)) {
                task.error("unexpected error", ExceptionUtils.getStackTrace(e));
                logger.error("unexpected error", e);
            }

            // 发生数据校验错误（不是异常错误), 再重新进行数据校验没有意义，重试任务时，需要重新传输数据
//            if (e instanceof VerificationFailed) {
//                task.setStatus(TaskStatus.DATA_FAILED);
//            }

            return;
        }

        task.setStatus(endStatus);
    }

    protected void withSubStatus(Enum startStatus, Enum endStatus, Enum errStatus, ActionFunc func) throws Exception {
        withSubStatus(startStatus.toString(), endStatus.toString(), errStatus.toString(), func);
    }

    protected void withSubStatus(String startStatus, String endStatus, String errStatus, ActionFunc func) throws Exception {
        task.setSubStatus(startStatus);

        try {
            func.run();
        } catch (Exception e) {
            task.setSubStatus(errStatus);
            throw e;
        }

        task.setSubStatus(endStatus);
    }

    protected void updateMigrationTargetStatus(MigrationStatus status) {
        if (partitionNumOfTask > 0) {
            partitionService.updatePartitionsStatus(status, task.getTaskModel().getId());
        } else {
            tableService.updateTableStatus(task.getTableId(), status);
        }
    }

    @Override
    public TaskType taskType() {
        return null;
    }

    @Override
    public void setUpSchema() {
        withStatus(
            TaskStatus.SCHEMA_DOING,
            TaskStatus.SCHEMA_DONE,
            TaskStatus.SCHEMA_FAILED,
            this::_setUpSchema
        );

        //task.log(String.format("create table %s if not exist", task.getOdpsTableFullName()), "");

        TableProxy table = task.getTable();
        if (table.isPartitionedTable() && task.getPartitions().isEmpty()) {
            logger.info("{} has no partitions, only table creation is needed", task.getTaskName());
            task.setTaskEnd();
            publishTaskEvent();
            updateMigrationTargetStatus(MigrationStatus.DONE);
        }
    }

    protected void dataTruncate() {
        withStatus(TaskStatus.DATA_DOING, TaskStatus.DATA_DONE, TaskStatus.DATA_FAILED, this::_dataTruncate);
    }

    @Override
    public void dataTrans() {
        withStatus(
            TaskStatus.DATA_DOING,
            TaskStatus.DATA_DONE,
            TaskStatus.DATA_FAILED,
            this::_dataTrans
        );
    }

    @Override
    public void verifyData() {
        withStatus(
            TaskStatus.VERIFICATION_DOING,
            TaskStatus.VERIFICATION_DONE,
            TaskStatus.VERIFICATION_FAILED,
            this::_verifyData
        );
    }

    protected void _setUpSchema() throws Exception {

    }

    protected void _dataTruncate() throws Exception {
    }

    protected void _dataTrans() throws Exception {

    }

    protected void _verifyData() throws Exception {

    }

    public void killSelf() {
        stopped = true;
    }

    protected void publishTaskEvent() {
        publisher.publishEvent(new TaskEvent(this, task.getTaskModel()));
    }

    @FunctionalInterface
    public interface ActionFunc {
        void run() throws Exception;
    }
}
