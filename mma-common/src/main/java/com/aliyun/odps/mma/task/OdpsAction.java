package com.aliyun.odps.mma.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.orm.TableProxy;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.sql.OdpsSql;
import com.aliyun.odps.mma.util.MutexFileLock;
import com.aliyun.odps.mma.util.OdpsUtils;
import com.aliyun.odps.task.SQLTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 对 task 对应的某一个 table 进行操作的辅助类
 */
public class OdpsAction {

    protected final OdpsUtils odpsUtils;
    protected final TaskProxy task;
    protected final OdpsSql odpsSql;
    protected final String projectName;
    protected final String tableName;
    protected final String tableFullName;
    protected final boolean hasPartition;

    // 创建dst端的odps action
    public OdpsAction(OdpsUtils odpsUtils, TaskProxy task) {
        this(odpsUtils,
             task,
             task.getOdpsProjectName(),
             task.getOdpsTableName(),
             task.getOdpsTableFullName()
        );
    }

    // 这里单独指定projectName, tableName, tableFullName是为了在源odps端执行sql，这时projectName等都需要指定为源project的
    public OdpsAction(OdpsUtils odpsUtils, TaskProxy task, String projectName, String tableName, String tableFullName) {
        this.odpsUtils = odpsUtils;
        this.task = task;
        this.projectName = projectName;
        this.tableName = tableName;
        this.tableFullName = tableFullName;
        this.hasPartition = task.getPartitions().size() > 0;
        this.odpsSql = new OdpsSql(task.getOdpsPartitionValues(), task.getTable().isPartitionedTable(),  hasPartition);
    }

    public static OdpsAction getSourceOdpsAction(OdpsConfig config, TaskProxy task) {
        return new OdpsAction(
                OdpsUtils.fromConfig(config),
                task,
                task.getDbName(),
                task.getTableName(),
                task.getTableFullName()
        );
    }


    public void createTableIfNotExists() throws MMATaskInterruptException {
        Map<String, String> hints = null;
        String ODPS_SQL_DECIMAL_ODPS2 = "odps.sql.decimal.odps2";

        TableProxy table = this.task.getTable();
        if (task.getJobConfig().getSourceConfig().getSourceType().equals(SourceType.ODPS) && table.hasDecimalColumn()) {
            hints = new HashMap<>();

            if (table.decimalOdps2()) {
                hints.put(ODPS_SQL_DECIMAL_ODPS2, "true");
            } else {
                hints.put(ODPS_SQL_DECIMAL_ODPS2, "false");
            }
        }

        createTableIfNotExists(hints);
    }

    public void createTableIfNotExists(Map<String, String> hints) throws MMATaskInterruptException {
        task.log(task.getCreateTableSql(), "try to create table: " + task.getOdpsTableFullName());

        wrapWithTryCatch(String.format("create odps table %s", tableFullName), () -> {
            odpsUtils.getOdps()
                    .tables()
                    .create(
                            projectName,
                            tableName,
                            task.getOdpsTableSchema(),
                            null,
                            true,
                            null,
                            hints,
                            null
                    );
        });
    }

    public void addPartitions() throws MMATaskInterruptException {
        if (!hasPartition) {
            return;
        }
        String sql = odpsSql.addPartitionsSql(tableFullName);

        wrapWithTryCatch(sql, () -> {
            try (MutexFileLock fileLock = new MutexFileLock(tableFullName)) {
                fileLock.lock();
                executeSql(sql);
            }
        });
    }

    public void truncate() throws MMATaskInterruptException {
        String sql = odpsSql.truncateSql(tableFullName);

        wrapWithTryCatch(sql, () -> {
            try (MutexFileLock fileLock = new MutexFileLock(tableFullName)) {
                fileLock.lock();
                executeSql(sql);
            }
        });
    }
    /**
     * @param hints
     * @throws MMATaskInterruptException
     */
    public void insertOverwrite(Map<String, String> hints, Consumer<Instance> insGetter) throws MMATaskInterruptException {
        // 源和目的 name 固定
        String sql = odpsSql.insertOverwriteSql(task.getTableFullName(), task.getOdpsTableFullName());
        wrapWithTryCatch(sql, () -> {
            Instance instance = executeSql(sql, hints);
            insGetter.accept(instance);
        });
    }

    public CompletableFuture<Long> selectCount(Consumer<Instance> insGetter) {
        return selectCount(insGetter, null);
    }

    public CompletableFuture<Long> selectCount(Consumer<Instance> insGetter, Map<String, String> hints) {
        return getCountFuture(odpsSql.selectCountSql(tableFullName), insGetter, hints);
    }

    protected CompletableFuture<Long> getCountFuture(String sql, Consumer<Instance> insGetter, Map<String, String> hints) {
        return CompletableFuture.supplyAsync(() -> {
            try {

                AtomicLong recordCount = new AtomicLong();

                wrapWithTryCatch(sql, () -> {
                    Instance instance = executeSql(sql, hints);
                    if (Objects.nonNull(insGetter)) {
                        insGetter.accept(instance);
                    }

                    List<Record> records = SQLTask.getResult(instance);

                    for (Record record : records) {
                        recordCount.addAndGet(Long.parseLong(record.getString(record.getColumnCount() - 1)));
                    }
                });

                return recordCount.get();

            } catch (MMATaskInterruptException e) {
                return -1L;
            }
        });
    }

    public String getBearToken() throws MMATaskInterruptException {
        StringBuilder sb = new StringBuilder();
        wrapWithTryCatch("get bearer token", () -> {
            sb.append(this.odpsUtils.getBearerToken(projectName, tableName));
        });

        return sb.toString();
    }


    protected Instance executeSql(String sql) throws OdpsException {
        return executeSql(sql, null);
    }

    protected Instance executeSql(String sql, Map<String, String> hints) throws OdpsException {
        task.log(sql, "start executing");
        Instance instance = odpsUtils.executeSql(sql, hints);
        String logView = odpsUtils.getLogView(instance);
        task.log(sql, logView);
        instance.waitForSuccess();
        return instance;
    }

    protected void wrapWithTryCatch(String action, ActionFunc actionFunc) throws MMATaskInterruptException {
        try {
            actionFunc.call();
        } catch (Exception e) {
            task.error(action, e);
            throw new MMATaskInterruptException();
        }
    }

    @FunctionalInterface
    protected static interface ActionFunc {
        void call() throws Exception;
    }
}
