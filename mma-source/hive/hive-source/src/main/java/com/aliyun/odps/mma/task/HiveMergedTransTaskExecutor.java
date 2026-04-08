package com.aliyun.odps.mma.task;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.meta.HiveUtils;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.orm.TableProxy;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.mma.util.OdpsAuthType;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.aliyun.odps.mma.util.PebbleUtils.renderTpl;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveMergedTransTaskExecutor extends MergedTransportTaskExecutor {
    private static final String UDTF_TPL_FILE = "tpl/udtf.peb";
    private static final String COUNT_TPL_FILE = "tpl/count.peb";


    HiveUtils hiveUtils;
    Connection sqlConn;

    AtomicLong hiveCnt = new AtomicLong(0);

    boolean countByUDTF = false;


    @Override
    public TaskType taskType() {
        return TaskType.HIVE_MERGED_TRANS;
    }

    protected void setUp() {
        hiveUtils = new HiveUtils((HiveConfig) sourceConfig);
    }

    @Override
    protected void _setUpSchema() throws Exception {
        odpsAction.createTableIfNotExists();
    }

    @Override
    public void _dataMergedTrans() throws Exception {
        countByUDTF = true;
        executeQuery(getUDTFSql());
    }

    @Override
    protected void _verifyData() throws Exception {
        if (! jobConfig.isEnableVerification()) {
            return;
        }

        CompletableFuture<Long> odpsCountFuture;

        if (! jobConfig.isNoUnMergePartition()) {
            odpsCountFuture = odpsAction.selectMergedCount(
                    task.getOdpsTableFullName(),
                    (ins) -> this.odpsIns = ins
            );
        } else {
            odpsCountFuture = odpsAction.selectMergedCount(
                    getTempTableFullName(),
                    (ins) -> this.odpsIns = ins
            );
        }

        if (!countByUDTF) {
            CompletableFuture<Void> hiveCountFuture = CompletableFuture.runAsync(() -> {
                try {
                    executeQuery(getCountSql());
                } catch (MMATaskInterruptException e) {
                    hiveCnt.set(-1);
                }
            });
            hiveCountFuture.join();
        }

        VerificationAction.countResultCompare("hive", hiveCnt.get(),
                "odps", odpsCountFuture.get(),
                task);
    }

    public void executeQuery(String sql) throws MMATaskInterruptException {
        try {
            task.log(sql, "start to execute hive sql, yarn application name is " + task.getTaskName());
            hiveUtils.executeQuery(
                    sql, task.getTaskName(), task.getJobConfig().getHiveSettings(),
                    (conn) -> this.sqlConn = conn,
                    // 一个sql可能会返回多个结果，要把所有结果累加起来
                    rs -> hiveCnt.addAndGet(rs.getLong(1))
            );
            task.log(sql,  Long.toString(hiveCnt.get()));
        } catch (SQLException e) {
            if (!this.stopped.get()) {
                task.error(sql, e);
                logger.warn("execute sql error for table " + task.getTaskName() , e);
            }
            throw new MMATaskInterruptException();
        }
    }

    public String getUDTFSql() throws MMATaskInterruptException {
        // get hive & odps column name list
        TableSchema odpsTableSchema = task.getDstOdpsTableSchema();
        List<String> odpsColumnNames = ListUtils.map(odpsTableSchema.getColumns(), Column::getName);
        List<String> odpsPartitionColumns = ListUtils.map(odpsTableSchema.getPartitionColumns(), Column::getName);
        odpsColumnNames.addAll(odpsPartitionColumns);

        MMATableSchema hiveTableSchema = task.getTable().getTableSchema();
        List<String> hiveColumnNames = ListUtils.map(hiveTableSchema.getColumns(), (c) -> String.format("`%s`", c.getName()));
        hiveColumnNames.addAll(ListUtils.map(hiveTableSchema.getPartitions(), (c) -> String.format("`%s`", c.getName())));

        Map<String, Object> ctx = new HashMap<>(12);
        ctx.put("functionName", sourceConfig.getOrDefault(HiveConfig.HIVE_UDTF_NAME,
                                                          "default.odps_data_dump_multi"));
        ctx.put("authType", OdpsAuthType.BearerToken);
        ctx.put("authInfo", this.odpsAction.getBearerToken(task.getOdpsProjectName(), task.getOdpsSchemaName(), getTempTableName()));
        ctx.put("mcEndpoint", mmaConfig.getMcDataEndpoint());
        ctx.put("tunnelEndpoint", mmaConfig.getConfig(MMAConfig.MC_TUNNEL_ENDPOINT));
        ctx.put("odpsProject", task.getOdpsProjectName());
        ctx.put("odpsSchemaName", task.getOdpsSchemaName());
        ctx.put("odpsTable", getTempTableName());
        ctx.put("odpsColumnNames", odpsColumnNames);
        ctx.put("odpsPartitionColumns", Collections.emptyList());
        ctx.put("hiveColumnNames", hiveColumnNames);
        ctx.put("hiveDb", task.getDbName());
        ctx.put("hiveTable", task.getTableName());
        ctx.put("partitionSpecs", Collections.emptyList());

        return renderTpl(UDTF_TPL_FILE, ctx);
    }

    public String getCountSql() {
        TableProxy table = task.getTable();

        Map<String, Object> ctx = new HashMap<>(10);
        ctx.put("hiveDb", table.getDbName());
        ctx.put("hiveTable", table.getName());
        ctx.put("partitionSpecs", Collections.emptyList());
        return renderTpl(COUNT_TPL_FILE, ctx);
    }

    @Override
    public void killSelf() {
        super.killSelf();

        if (Objects.nonNull(sqlConn)) {
            try {
                sqlConn.close();
                sqlConn = null;
            } catch (Exception _e) {
                // ignore
            }
        }
    }
}
