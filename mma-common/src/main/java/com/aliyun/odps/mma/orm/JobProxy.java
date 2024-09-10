package com.aliyun.odps.mma.orm;

import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.config.PartitionFilter;
import com.aliyun.odps.mma.constant.*;
import com.aliyun.odps.mma.execption.JobConfigException;
import com.aliyun.odps.mma.execption.JobSubmittingException;
import com.aliyun.odps.mma.model.*;
import com.aliyun.odps.mma.service.*;
import com.aliyun.odps.mma.util.KeyLock;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.mma.util.TableHasher;
import com.aliyun.odps.mma.util.TableName;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JobProxy {
    private static final Logger logger = LoggerFactory.getLogger(JobProxy.class);
    private final MMAConfig mmaConfig;
    private final JobService jobService;
    private final DbService dbService;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final TaskService taskService;
    private final DataSourceService dataSourceService;

    private JobModel jobModel;
    @Getter
    private JobConfig jobConfig;

    protected void init(JobModel jobModel) {
        this.jobModel = jobModel;
        this.jobConfig = this.jobModel.getConfig();
        this.jobConfig.setMmaConfig(this.mmaConfig);

        String dataSourceName = this.jobModel.getSourceName();
        DataSourceModel dataSourceModel = dataSourceService.getDataSource(dataSourceName).get();
        this.jobConfig.setSourceConfig(dataSourceModel.getConfig());
    }

    @Autowired
    public JobProxy(
            MMAConfig config,
            JobService jobService,
            DbService dbService,
            TableService tableService,
            PartitionService partitionService,
            TaskService taskService,
            DataSourceService dataSourceService
    ) {
        this.jobService = jobService;
        this.mmaConfig = config;
        this.dbService = dbService;
        this.tableService = tableService;
        this.partitionService = partitionService;
        this.taskService = taskService;
        this.dataSourceService = dataSourceService;
    }

    public int submit() throws Exception {
        String dsName = jobModel.getSourceName();
        String dbName = jobModel.getDbName();
        String lockKey = String.format("lock_%s.%s", dsName, dbName);

        try (KeyLock keyLock = new KeyLock(lockKey)) {
            keyLock.lock();
            return _submit();
        }
    }

    private int _submit() throws JobSubmittingException {
        String dsName = jobModel.getSourceName();
        String dbName = jobModel.getDbName();

        TaskType taskType = jobConfig.getTaskType();

        if (taskType == TaskType.ODPS && jobConfig.getMaxPartitionLevel() >= 0) {
            throw new JobSubmittingException("不支持合并分区");
        }

        switch (this.jobModel.getType()) {
            case Database:
                DataBaseModel db = dbService.getDbByName(dsName, dbName).get();
                return submitDb(db);
            case Tables:
                List<TableModel> tables = tableService.getTablesOfDbByNames(dsName, dbName, jobConfig.getTables());
                return submitTables(tables);
            case Partitions:
                List<PartitionModel> partitions = partitionService.getPartitions(jobConfig.getPartitions());
                return submitPartitions(partitions);
        }

        throw new JobSubmittingException(
                String.format(
                        "unexpected job type, should be one of (%s)",
                        Arrays.stream(JobType.values()).map(Objects::toString).collect(Collectors.joining(", "))
                )
        );
    }

    public int submitDb(DataBaseModel db) throws JobSubmittingException {
        // 获取db的tables
        List<String> whiteList = jobConfig.getTableWhiteList();
        List<String> blackList = jobConfig.getTableBlackList();

        List<TableModel> tables = null;

        if (ListUtils.size(whiteList) > 0) {
            tables = tableService.getTablesOfDbWithWhiteList(db.getId(), whiteList);
        } else if (ListUtils.size(blackList) > 0){
            tables = tableService.getTablesOfDbWithBlackList(db.getId(), blackList);
        } else {
            tables = tableService.getTablesOfDb(db.getId());
        }

        if (tables.isEmpty()) {
            throw new JobSubmittingException("there are no tables found");
        }

        return submitTables(tables);
    }

    public int submitTable(TableModel table) throws JobSubmittingException {
        List<PartitionModel> partitionModels = partitionService.getPartitionsOfTableBasic(table.getId());
        List<TaskModel> tasks = generateTasks(table, partitionModels);
        return submitTableTasks(tasks);
    }

    public int submitTables(List<TableModel> tables) throws JobSubmittingException {
        List<TaskModel> tasks = generateTasks(tables);
        return submitTableTasks(tasks);
    }

    private int submitTableTasks(List<TaskModel> tasks) throws JobSubmittingException {
        List<Integer> tableIds = new ArrayList<>();
        List<Integer> partitionIds = new ArrayList<>();

        for (TaskModel task: tasks) {
            List<Integer> _partitionIds = task.getPartitions();

            if (Objects.nonNull(_partitionIds) && !_partitionIds.isEmpty()) {
                partitionIds.addAll(_partitionIds);
            } else {
                tableIds.add(task.getTableId());
            }
        }

        logger.info("start to get running task for {} partitions and {} tables", partitionIds.size(), tableIds.size());
        List<TaskModel> existedTasks = taskService.getRunningTasks(partitionIds, tableIds);
        logger.info("success to get running task for {} partitions and {} tables", partitionIds.size(), tableIds.size());
        if (!existedTasks.isEmpty()) {
            throw newException(existedTasks);
        }

        jobService.submit(jobModel, tasks);
        return  jobModel.getId();
    }

    private List<TaskModel> generateTasks(List<TableModel> tables) throws JobSubmittingException {
        List<TaskModel> tasks = new ArrayList<>();

        logger.info("start to generate tasks for {} tables", tables.size());
        List<Integer> tableIds = tables.stream().map(TableModel::getId).collect(Collectors.toList());
        List<PartitionModel> partitions = partitionService.getPartitionsOfTablesBasic(tableIds);
        Map<Integer, List<PartitionModel>> partitionsOfTable = new HashMap<>();

        for (PartitionModel pm: partitions) {
            List<PartitionModel> ptsOfTable = partitionsOfTable.get(pm.getTableId());
            if (Objects.isNull(ptsOfTable)) {
                ptsOfTable = new LinkedList<>();
                partitionsOfTable.put(pm.getTableId(), ptsOfTable);
            }
            ptsOfTable.add(pm);
        }

        for (TableModel table: tables) {
            List<PartitionModel> ptsOfTable = partitionsOfTable.get(table.getId());
            tasks.addAll(generateTasks(table, ptsOfTable));
        }

        logger.info("success to generate tasks for {} tables", tables.size());

        return tasks;
    }

    private List<TaskModel> generateTasks(TableModel table, List<PartitionModel> _partitions) throws JobSubmittingException {
        if (Objects.isNull(_partitions)) {
            _partitions = new ArrayList<>();
        }

        List<TaskModel> tasks = new ArrayList<>();

        TaskModel.TaskModelBuilder tb = TaskModel.builder();
        TableName odpsTable = jobConfig.getDstOdpsTable(table.getName(), jobModel.getDstOdpsSchema(), jobModel.getDstOdpsProject());

        tb.dbName(table.getDbName())
                .sourceId(table.getSourceId())
                .dbId(table.getDbId())
                .schemaName(table.getSchemaName())
                .tableName(table.getName())
                .tableId(table.getId())
                .odpsProject(odpsTable.getDbName())
                .odpsSchema(odpsTable.getSchemaName())
                .odpsTable(odpsTable.getName())
                .type(jobConfig.getTaskType())
                .status(TaskStatus.INIT);

        // 处理非分区表
        if (!table.isHasPartitions()) {
            // 有”定时任务 + 增量更新“时，如果已经迁移成功的非分区表有变动，则重新迁移
            if (jobModel.isOldJob() && jobConfig.isIncrement() && MigrationStatus.DONE == table.getStatus()) {
                if (table.isUpdated()) {
                    TaskModel task = tb.build();
                    tasks.add(task);
                    return tasks;
                }

                logger.info("there no task for unPartitioned table {} in timer job {}", table.getFullName(), jobModel.getId());
                return new ArrayList<>();
            }

            // 其他情况
            TaskModel task = tb.build();
            tasks.add(task);
            return tasks;
        }

        List<PartitionModel> partitions = _partitions;

        Optional<PartitionFilter> pfOpt = jobConfig.getPartitionFilter(table.getName());

        if (pfOpt.isPresent()) {
            PartitionFilter partitionFilter = pfOpt.get();
            partitions = new ArrayList<>();

            for (PartitionModel partition: _partitions) {
                boolean isWanted = partitionFilter.filter(partition.getValue(
                    jobConfig.getSourceConfig()));
                Optional<String> errOpt = partitionFilter.getTypeError();
                if (errOpt.isPresent()) {
                    throw new JobConfigException("partition_filter", errOpt.get());
                }

                if (isWanted) {
                    partitions.add(partition);
                }
            }

            if (partitions.isEmpty()) {
                throw new JobConfigException(
                        "partition_filter",
                        "there are no partitions filter out by " + partitionFilter.getFilterExpr()
                );
            }
        }

        if (jobConfig.isIncrement()) {
            partitions = partitions
                    .stream()
                    .filter(p -> {
//                        // 定时任务只迁未建立过任务的分区, 否则任务重试的时候可能有多个失败的指向同一个分区
//                        if (jobModel.isOldJob()) {
//                            return MigrationStatus.INIT.equals(p.getStatus());
//                        }
//
                        return (MigrationStatus.DONE != p.getStatus() && MigrationStatus.DOING != p.getStatus())
                                || p.isUpdated();
                    })
                    .collect(Collectors.toList());
        }

        // 分区表如果没有分区的话只建schema
        if (partitions.isEmpty()) {
            TaskModel task = tb.build();
            tasks.add(task);

            // 增量更新 + 定时任务场景，如果表已经迁移完毕，并且没有新的分区，则不产生新的任务
            if (jobModel.isOldJob() && MigrationStatus.DONE == table.getStatus()) {
                logger.info("there no task for table {} in timer job {}", table.getFullName(), jobModel.getId());
                return new ArrayList<>();
            }

            return tasks;
        }

        List<List<PartitionModel>> partitionGroups = jobConfig.getPartitionGrouping().group(partitions);
        for (List<PartitionModel> partitionGroup : partitionGroups) {
            List<Integer> partitionIds = partitionGroup
                    .stream()
                    .map(PartitionModel::getId)
                    .collect(Collectors.toList());

            tb.partitions(partitionIds);
            TaskModel task = tb.build();
            tasks.add(task);
        }

        return tasks;
    }

    public int submitPartition(PartitionModel pm) throws JobSubmittingException {
        List<PartitionModel> ps = new ArrayList<>(1);
        ps.add(pm);

        return this.submitPartitions(ps);
    }

    public int submitPartitions(List<PartitionModel> ps) throws JobSubmittingException {
        // 校验是否已经有task在操作或准备操作partition
        List<Integer> partitionIds = ps.stream().map(PartitionModel::getId).collect(Collectors.toList());
        List<TaskModel> existedTasks = taskService.getRunningTasks(partitionIds);
        if (!existedTasks.isEmpty()) {
            throw newException(existedTasks);
        }

        // 按照table对分区进行分组
        Map<TableHasher, List<PartitionModel>> groupsByTable = new HashMap<>();

        for (PartitionModel pm : ps) {
            TableHasher tableHash = pm.tableTableHasher();
            List<PartitionModel> groups = groupsByTable.get(tableHash);

            if (Objects.nonNull(groups)) {
                groups.add(pm);
            } else {
                groups = new LinkedList<>();
                groups.add(pm);
                groupsByTable.put(tableHash, groups);
            }
        }

        List<TaskModel> tasks = new LinkedList<>();

        // 以table为单位，对一个table内的分区进行分组，每个分区分组组成一个tasks
        for (TableHasher th : groupsByTable.keySet()) {
            List<PartitionModel> groupOfTable = groupsByTable.get(th);
            List<List<PartitionModel>> ptGroups = this.jobConfig.getPartitionGrouping().group(groupOfTable);

            String dbName = th.getDbName();
            String schemName = th.getSchemName();
            String tableName = th.getTableName();
            Integer sourceId = groupOfTable.get(0).getSourceId();
            Integer dbId = groupOfTable.get(0).getDbId();
            Integer tableId = groupOfTable.get(0).getTableId();
            TaskType taskType = jobConfig.getTaskType();
            TableName odpsTable = jobConfig.getDstOdpsTable(tableName, jobModel.getDstOdpsSchema(), jobModel.getDstOdpsProject());

            ptGroups
                    .stream()
                    .map(ptGroup -> ptGroup.stream().map(PartitionModel::getId).collect(Collectors.toList()))
                    .forEach(ptGroup -> {
                        TaskModel.TaskModelBuilder tb = TaskModel.builder();

                        tb.dbName(dbName)
                                .sourceId(sourceId)
                                .dbId(dbId)
                                .schemaName(schemName)
                                .tableName(tableName)
                                .tableId(tableId)
                                .odpsProject(odpsTable.getDbName())
                                .odpsSchema(odpsTable.getSchemaName())
                                .odpsTable(odpsTable.getName())
                                .type(taskType)
                                .status(TaskStatus.INIT)
                                .partitions(ptGroup);

                        tasks.add(tb.build());
                    });
        }

        jobService.submit(jobModel, tasks);
        return jobModel.getId();
    }

    public int getJobId() {
        return this.jobModel.getId();
    }

    public JobConfig getMmaConfig() {
        JobConfig jc = jobModel.getConfig();
        return jc;
    }

    public Map<String, String> getTableMapping() {
        Map<String, String> mapping = jobModel.getConfig().getTableMapping();

        if (Objects.nonNull(mapping)) {
            return mapping;
        }

        return new HashMap<>();
    }

    public TaskType getTaskType() {
        return jobModel.getConfig().getTaskType();
    }

    public List<Integer> getPartitions() {
        return jobModel.getConfig().getPartitions();
    }

    public List<String> getTableNames() {
        return jobConfig.getTables();
    }

    public String getDbName() {
        return jobModel.getDbName();
    }

    public String getOdpsProject() {
        return jobModel.getDstOdpsProject();
    }

    private JobSubmittingException newException(List<TaskModel> tasks) {
        String msg = String.join(
                ", ",
                tasks
                        .stream()
                        .map(taskModel -> String.format("%s.%s", taskModel.getDbName(), taskModel.getTableName()))
                        .collect(Collectors.toSet())
        );

        // TODO 创建jobBatch
        String errMsg = String.format("There are tasks those are running or ready to run for %s", msg);
        if (jobModel.isOldJob()) {
            JobBatchModel jobBatch = JobBatchModel
                    .builder()
                    .jobId(jobModel.getId())
                    .status(JobBatchStatus.FAILED)
                    .errMsg(errMsg)
                    .build();

            jobService.insertJobBatch(jobBatch);
        }

        return new JobSubmittingException(errMsg);
    }
}

