package com.aliyun.odps.mma.task;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.aliyun.odps.Instance;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.service.DbService;
import com.aliyun.odps.mma.util.OdpsUtils;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class InsertOverwriteTaskExecutor extends TaskExecutor {
    Logger logger = LoggerFactory.getLogger(InsertOverwriteTaskExecutor.class);

    OdpsUtils sourceOdpsUtils;
    OdpsAction sourceOdpsAction;
    Instance dataTransInstance;
    Instance srcCountInstance;
    Instance destCountInstance;
    DbService dbService;

    @Autowired
    public InsertOverwriteTaskExecutor(DbService dbService) {
        super();
        this.dbService = dbService;
    }

    @Override
    protected void setUp() {
        sourceOdpsUtils = OdpsUtils.fromConfig(sourceConfig);
        sourceOdpsAction = new OdpsAction(sourceOdpsUtils,
                task,
                task.getDbName(),
                task.getTableName(),
                task.getTableFullName()
        );
    }

    @Override
    public TaskType taskType() {
        return TaskType.ODPS_INSERT_OVERWRITE;
    }

    @Override
    protected void _setUpSchema() throws Exception {
        odpsAction.createTableIfNotExists();
        //odpsAction.addPartitions();
    }

    @Override
    protected void _dataTruncate() throws Exception {
        //odpsAction.truncate();
    }

    @Override
    protected void _dataTrans() throws Exception {
        odpsAction.insertOverwrite(
                sourceConfig.getMap(OdpsConfig.MC_SQL_HINTS),
                instance -> dataTransInstance = instance
        );
    }

    @Override
    protected void _verifyData() throws Exception {
        CompletableFuture<Long> destCountFuture = odpsAction.selectCount((ins) -> this.destCountInstance = ins);
        CompletableFuture<Long> sourceCountFuture = sourceOdpsAction.selectCount(
                (ins) -> this.srcCountInstance = ins,
                sourceConfig.getMap(OdpsConfig.MC_SQL_HINTS)
        );
        sourceCountFuture.join();

        VerificationAction.countResultCompare(
                "src odps", sourceCountFuture.get(),
                "dest odps", destCountFuture.get(),
                task
        );
    }

    @Override
    public void killSelf() {
        super.killSelf();

        OdpsUtils.stop(dataTransInstance);
        dataTransInstance= null;
        OdpsUtils.stop(srcCountInstance);
        srcCountInstance = null;
        OdpsUtils.stop(destCountInstance);
        destCountInstance = null;
    }

}
