package com.aliyun.odps.mma.service;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Getter
public class Service {
    DbService dbService;
    TableService tableService;
    PartitionService partitionService;
    JobService jobService;
    TaskService taskService;
    DataSourceService dsService;

    @Autowired
    public void setDbService(DbService dbService) {
        this.dbService = dbService;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

    @Autowired
    public void setPartitionService(PartitionService partitionService) {
        this.partitionService = partitionService;
    }

    @Autowired
    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    @Autowired
    public void setTaskService(TaskService taskService) {
        this.taskService = taskService;
    }

    @Autowired
    public void setDsService(DataSourceService dsService) {
        this.dsService = dsService;
    }
}
