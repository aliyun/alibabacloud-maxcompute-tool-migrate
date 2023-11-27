package com.aliyun.odps.mma.task;

import static com.aliyun.odps.mma.util.PebbleUtils.renderTpl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.aliyun.odps.Instance;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.meta.HiveUtils;
import com.aliyun.odps.mma.util.ListUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.orm.TableProxy;
import com.aliyun.odps.mma.util.OdpsAuthType;
import com.aliyun.odps.mma.util.OdpsUtils;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveTaskExecutor extends TaskExecutor {
    Logger logger = LoggerFactory.getLogger(HiveTaskExecutor.class);

    HiveUtils hiveUtils;
    Connection sqlConn;
    Instance odpsIns;

    // hive端的行数可能来自于data trans udtf, 也可能来自于select count
    AtomicLong hiveCnt = new AtomicLong(0);
    boolean countByUDTF = false;

    private static final String UDTF_TPL_FILE = "tpl/udtf.peb";
    private static final String COUNT_TPL_FILE = "tpl/count.peb";

    public HiveTaskExecutor() {
        super();
    }

    @Override
    public TaskType taskType() {
        return TaskType.HIVE;
    }


    @Override
    protected void setUp() {
        hiveUtils = new HiveUtils((HiveConfig) sourceConfig);
    }

    @Override
    protected void _setUpSchema() throws Exception {
        odpsAction.createTableIfNotExists();
        odpsAction.addPartitions();
    }

    @Override
    protected void _dataTruncate() throws Exception {
        countByUDTF = true;
        odpsAction.truncate();
    }

    @Override
    protected void _dataTrans() throws Exception {
        executeQuery(getUDTFSql());
    }

    @Override
    protected void _verifyData() throws Exception {
        CompletableFuture<Long> odpsCountFuture = odpsAction.selectDstCount(
                task.getOdpsTableFullName(),
                (ins) -> this.odpsIns = ins
        );

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

        OdpsUtils.stop(odpsIns);
        odpsIns = null;
    }

    public void executeQuery(String sql) throws MMATaskInterruptException {
        try {
            task.log(sql, "start to execute hive sql, yarn application name is " + task.getTaskName());
            hiveUtils.executeQuery(
                    sql, task.getTaskName(), task.getJobConfig().getHiveSettings(),
                    (conn) -> this.sqlConn = conn,
                    rs -> hiveCnt.addAndGet(rs.getLong(1))
            );
            task.log(sql,  Long.toString(hiveCnt.get()));
        } catch (SQLException e) {
            if (!this.stopped) {
                task.error(sql, e);
                logger.warn("execute sql error for table " + task.getTaskName() , e);
            }
            throw new MMATaskInterruptException();
        }
    }

    public String getUDTFSql() throws MMATaskInterruptException {
        // get hive & odps column name list
        TableSchema odpsTableSchema = task.getOdpsTableSchema();
        List<String> odpsColumnNames = ListUtils.map(odpsTableSchema.getColumns(), Column::getName);
        List<String> odpsPartitionColumns = ListUtils.map(odpsTableSchema.getPartitionColumns(), Column::getName);

        JobConfig jobConfig = task.getJobConfig();
        int maxPartitionLevel = jobConfig.getMaxPartitionLevel();

        MMATableSchema hiveTableSchema = task.getTable().getTableSchema();

        List<MMAColumnSchema> columns = new ArrayList<>(hiveTableSchema.getColumns());
        List<MMAColumnSchema> ptColumns = new ArrayList<>(hiveTableSchema.getPartitions());

        if (maxPartitionLevel >= 0 && ptColumns.size() > maxPartitionLevel) {
            columns.addAll(ptColumns.subList(maxPartitionLevel, ptColumns.size()));
            ptColumns = ptColumns.subList(0, maxPartitionLevel);
        }

        List<String> hiveColumnNames = ListUtils.map(columns, (c) -> String.format("`%s`", c.getName()));
        hiveColumnNames.addAll(ListUtils.map(ptColumns, (c) -> String.format("`%s`", c.getName())));

        Map<String, Object> ctx = new HashMap<>(20);
        ctx.put("authType", OdpsAuthType.BearerToken);
        ctx.put("authInfo", this.odpsAction.getBearToken());
        ctx.put("mcEndpoint", mmaConfig.getMcDataEndpoint());
        ctx.put("tunnelEndpoint", mmaConfig.getConfig(MMAConfig.MC_TUNNEL_ENDPOINT));
        ctx.put("odpsProject", task.getOdpsProjectName());
        ctx.put("odpsTable", task.getOdpsTableName());
        ctx.put("odpsColumnNames", odpsColumnNames);
        ctx.put("odpsPartitionColumns", odpsPartitionColumns);
        ctx.put("hiveColumnNames", hiveColumnNames);
        ctx.put("hiveDb", task.getDbName());
        ctx.put("hiveTable", task.getTableName());
        ctx.put("partitionSpecs", getWhereConditionWithPartitions());

        return renderTpl(UDTF_TPL_FILE, ctx);
    }


    public String getCountSql() {
        TableProxy table = task.getTable();

        Map<String, Object> ctx = new HashMap<>(10);
        ctx.put("hiveDb", table.getDbName());
        ctx.put("hiveTable", table.getName());
        ctx.put("partitionSpecs", getWhereConditionWithPartitions());
        return renderTpl(COUNT_TPL_FILE, ctx);
    }

    private List<String> getWhereConditionWithPartitions() {
        return task.getSrcPartitionValues()
                .stream()
                .map(pv -> pv.transfer(
                        (name, type, value) -> String.format("`%s`=cast('%s' AS %s)", name, value, type),
                        " AND "
                ))
                .collect(Collectors.toList());
    }
}
