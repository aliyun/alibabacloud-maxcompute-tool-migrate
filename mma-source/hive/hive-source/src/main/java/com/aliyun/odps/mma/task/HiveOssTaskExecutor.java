package com.aliyun.odps.mma.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.config.HiveGlueConfig;
import com.aliyun.odps.mma.config.HiveOssConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.meta.HiveUtils;
import com.aliyun.odps.mma.orm.TableProxy;
import com.aliyun.odps.mma.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.aliyun.odps.mma.util.PebbleUtils.renderTpl;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveOssTaskExecutor extends TaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(HiveOssTaskExecutor.class);
    private HiveUtils hiveUtils;
    private OdpsOssActionForHive odpsAction;
    private Connection sqlConn;
    private Instance dataTransInstance;
    private Instance ossCountInstance;
    private Instance odpsCountInstance;

    private static final String COUNT_TPL_FILE = "tpl/count.peb";

    public HiveOssTaskExecutor() {
        super();
    }

    @Override
    protected void setUp() {
        hiveUtils = new HiveUtils((HiveConfig) sourceConfig);
        OdpsUtils odpsUtils = OdpsUtils.fromConfig(mmaConfig);
        this.odpsAction = new OdpsOssActionForHive(odpsUtils, task);
    }

    @Override
    public TaskType taskType() {
        return TaskType.HIVE_OSS;
    }

    @Override
    protected void _setUpSchema() throws Exception {
        odpsAction.createTableIfNotExists();
        switch (sourceConfig.getSourceType()) {
            case HIVE_OSS:
                HiveOssConfig hiveOssConfig = (HiveOssConfig) sourceConfig;
                String location = hiveOssConfig.getTableDataPath(task.getDbName(), task.getTableName());
                odpsAction.createExternalTable(location);
                break;
            case HIVE_GLUE:
                String s3Location = task.getTable().getTableModel().getLocation();
                HiveGlueConfig hiveGlueConfig = (HiveGlueConfig) sourceConfig;
                String ossLocation = hiveGlueConfig.getTableDataPathFromS3(s3Location);
                odpsAction.createExternalTable(ossLocation);
                break;
        }

        odpsAction.addPartitionsToExternalTable();
    }

    @Override
    protected void _dataTruncate() {
        // insert overwrite don't need truncate
    }

    @Override
    protected void _dataTrans() throws Exception {
        try {
            odpsAction.insertOverwriteFromOssToMc(
                    sourceConfig.getMap(HiveOssConfig.MC_SQL_HINTS),
                    (ins) -> this.dataTransInstance = ins
            );
        } catch (MMATaskInterruptException e) {
            throw e;
        }
    }

    @Override
    protected void _verifyData() throws Exception {
        CompletableFuture<Long> hiveCountFuture = hiveSelectCount();
        CompletableFuture<Long> odpsCountFuture = odpsAction.selectCount(
                task.getOdpsTableFullName(),
                (ins) -> this.odpsCountInstance = ins
        );
//        CompletableFuture<Long> externalCountFuture = odpsAction.selectCountExternalTable(
//                (ins) -> this.ossCountInstance = ins
//        );

        hiveCountFuture.join();
//        externalCountFuture.join();

//        VerificationAction.countResultCompare(
//                "odps",
//                odpsCountFuture.get(),
//                "oss",
//                externalCountFuture.get(),
//                task
//        );

        VerificationAction.countResultCompare(
                "hive",
                hiveCountFuture.get(),
                "odps",
                odpsCountFuture.get(),
                task
        );
    }

    @Override
    public void killSelf() {
        super.killSelf();
        OdpsUtils.stop(this.dataTransInstance);
        this.dataTransInstance = null;
        OdpsUtils.stop(ossCountInstance);
        ossCountInstance = null;
        OdpsUtils.stop(odpsCountInstance);
        odpsCountInstance = null;
    }

    private CompletableFuture<Long> hiveSelectCount() throws MMATaskInterruptException {
        return CompletableFuture.supplyAsync(() -> {
            String sql = getCountSql();
            AtomicLong hiveCnt = new AtomicLong(0);

            try {
                task.log(sql, "start to execute hive sql, yarn application name is " + task.getTaskName());

                hiveUtils.executeQuery(
                        sql, task.getTaskName(), task.getJobConfig().getHiveSettings(),
                        (conn) -> this.sqlConn = conn,
                        rs -> hiveCnt.addAndGet(rs.getLong(1))
                );
                task.log(sql,  Long.toString(hiveCnt.get()));
            } catch (SQLException e) {
                hiveCnt.set(-1);
                if (!this.stopped) {
                    task.error(sql, e);
                    logger.warn("execute sql error for table " + task.getTaskName() , e);
                }
            }

            return hiveCnt.get();
        });
    }

    private String getCountSql() {
        TableProxy table = task.getTable();

        Map<String, Object> ctx = new HashMap<>(10);
        ctx.put("hiveDb", table.getDbName());
        ctx.put("hiveTable", table.getName());
        ctx.put("ptColumns", table.getTableSchema().getPartitions());
        ctx.put("partitionSpecs", getWhereConditionWithPartitions());
        return renderTpl(COUNT_TPL_FILE, ctx);
    }

    private List<String> getWhereConditionWithPartitions() {
        return task.getSrcPartitionValues()
                .stream()
                .map(pv -> pv.transfer(
                        (name, type, value) -> String.format("%s=cast('%s' AS %s)", name, value, type),
                        " AND "
                ))
                .collect(Collectors.toList());
    }
}
