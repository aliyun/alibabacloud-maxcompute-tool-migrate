package com.aliyun.odps.mma.orm;

import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.service.JobService;
import com.aliyun.odps.mma.service.Service;
import com.aliyun.odps.mma.service.TableService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;


@Component
public class OrmFactory {
    Service service;
    ApplicationContext appCtx;
    MMAConfig config;

    @Autowired
    public OrmFactory(ApplicationContext appCtx, Service service, MMAConfig mmaConfig) {
        this.appCtx = appCtx;
        this.service = service;
        this.config = mmaConfig;
    }

    public JobProxy newJobProxy(JobModel jobModel) {
        JobProxy jobProxy = appCtx.getBean(JobProxy.class);
        jobProxy.init(jobModel);

        return jobProxy;
    }

    public TaskProxy newTaskProxy(TaskModel taskModel) {
        TableService tableService = service.getTableService();
        TableModel tableModel = tableService.getTableById(taskModel.getTableId());
        JobService jobService = service.getJobService();
        JobModel jobModel = jobService.getJobById(taskModel.getJobId());
        TableProxy tableProxy = newTableProxy(tableModel);
        TaskProxy task = appCtx.getBean(TaskProxy.class);

        JobConfig jobConfig = jobModel.getConfig();
        jobConfig.setMmaConfig(config);

        String dataSourceName = jobModel.getSourceName();
        DataSourceModel dataSourceModel = service.getDsService().getDataSource(dataSourceName).get();
        jobConfig.setSourceConfig(dataSourceModel.getConfig());

        task.init(taskModel, jobModel, jobConfig, tableProxy);

        return task;
    }

    public TableProxy newTableProxy(TableModel tableModel) {
        TableProxy table = appCtx.getBean(TableProxy.class);
        table.init(tableModel);

        return table;
    }

    public MMADataSource newDataSource(String name) {
        MMADataSource ds = appCtx.getBean(MMADataSource.class);
        ds.init(name);
        return ds;
    }

    public MMADataSource newDataSource(DataSourceModel dm) {
        MMADataSource ds = appCtx.getBean(MMADataSource.class);
        ds.init(dm);

        return ds;
    }
}

