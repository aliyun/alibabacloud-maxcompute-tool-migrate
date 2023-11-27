package com.aliyun.odps.mma.task;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.orm.TableProxy;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.sql.OdpsSqlUtils;
import com.aliyun.odps.mma.sql.PartitionValue;
import com.aliyun.odps.mma.util.KeyLock;
import com.aliyun.odps.mma.util.OdpsUtils;
import com.aliyun.odps.task.SQLTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 对 task 对应的某一个 table 进行操作的辅助类
 */
public class OdpsAction {

    protected final OdpsUtils odpsUtils;
    protected final TaskProxy task;

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
        String sql = OdpsSqlUtils.createTableSql(
                task.getOdpsProjectName(),
                task.getOdpsTableName(),
                task.getOdpsTableSchema(),
                null,
                task.getTable().getLifeCycle()

        );

        task.log(sql, "try to create table: " + task.getOdpsTableFullName());

        wrapWithTryCatch(sql, ()-> {
            executeSql(sql, hints);
        });
    }

    public void addPartitions() throws MMATaskInterruptException {
        if (! task.getTable().isPartitionedTable()) {
            return;
        }

        List<PartitionValue> ptValues = task.getDstOdpsPartitionValues();
        if (ptValues.isEmpty()) {
            return;
        }

        String odpsTableFullName = task.getOdpsTableFullName();

        String sql = OdpsSqlUtils.addPartitionsSql(
                odpsTableFullName,
                ptValues
        );

        wrapWithTryCatch(sql, () -> {
            try (KeyLock keyLock = new KeyLock(odpsTableFullName)) {
                keyLock.lock();
                executeSql(sql);
            }
        });
    }

    public void truncate() throws MMATaskInterruptException {
        String odpsTableFullName = task.getOdpsTableFullName();

        String sql = OdpsSqlUtils.truncateTableOrPartitionsSql(
                odpsTableFullName,
                task.getDstOdpsPartitionValues()
        );

        wrapWithTryCatch(sql, () -> {
            try (KeyLock keyLock = new KeyLock(odpsTableFullName)) {
                keyLock.lock();
                executeSql(sql);
            }
        });
    }
    /**
     * @param hints
     * @throws MMATaskInterruptException
     */
    public void insertOverwrite(Map<String, String> hints, Consumer<Instance> insGetter) throws MMATaskInterruptException {
        TableSchema schema = task.getOdpsTableSchema();

        // 源和目的 name 固定
        String sql = OdpsSqlUtils.insertOverwriteSql(
                task.getTableFullName(),
                task.getOdpsTableFullName(),
                schema,
                task.getSrcPartitionValues()
        );

        wrapWithTryCatch(sql, () -> {
            Instance instance = executeSql(sql, hints);
            insGetter.accept(instance);
        });
    }

    public CompletableFuture<Long> selectCount(String tableFullName, Consumer<Instance> insGetter) {
        return selectCount(tableFullName, insGetter, null);
    }

    public CompletableFuture<Long> selectSrcCount(String tableFullName, Consumer<Instance> insGetter) {
        return selectSrcCount(tableFullName, insGetter, null);
    }

    public CompletableFuture<Long> selectDstCount(String tableFullName, Consumer<Instance> insGetter) {
        return selectDstCount(tableFullName, insGetter, null);
    }

    public CompletableFuture<Long> selectCount(String tableFullName, Consumer<Instance> insGetter, Map<String, String> hints) {
        String sql = OdpsSqlUtils.selectCountSql(tableFullName, task.getSrcPartitionValues());
        return getCountFuture(sql, insGetter, hints);
    }

    public CompletableFuture<Long> selectSrcCount(String tableFullName, Consumer<Instance> insGetter, Map<String, String> hints) {
        String sql = OdpsSqlUtils.selectCountSql(tableFullName, task.getSrcPartitionValues());
        return getCountFuture(sql, insGetter, hints);
    }

    public CompletableFuture<Long> selectDstCount(String tableFullName, Consumer<Instance> insGetter, Map<String, String> hints) {
        String sql = OdpsSqlUtils.selectCountSql(tableFullName, task.getDstOdpsPartitionValues());
        return getCountFuture(sql, insGetter, hints);
    }

    public CompletableFuture<Long> selectMergedCount(String tableFullName, Consumer<Instance> insGetter) {
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.sql.allow.fullscan", "true");
        String sql = OdpsSqlUtils.selectCountSql(tableFullName, null);
        return getCountFuture(sql, insGetter, hints);
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

                    List<Record> records = SQLTask.getResult(instance, "MMAv3");

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
        return getBearToken(task.getOdpsProjectName(), task.getOdpsTableName());
    }

    public String getBearToken(String projectName, String tableName) throws MMATaskInterruptException {
        StringBuilder sb = new StringBuilder();
        wrapWithTryCatch("get bearer token", () -> {
            sb.append(this.odpsUtils.getBearerToken(projectName, tableName));
        });

        return sb.toString();
    }


    public Instance executeSql(String sql) throws OdpsException {
        return executeSql(sql, null);
    }

    public Instance executeSql(String sql, Map<String, String> hints) throws OdpsException {
        task.log(sql, "start executing");
        Instance instance = odpsUtils.executeSql(sql, hints);
        String logView = odpsUtils.getLogView(instance);
        task.log(sql, logView);
        instance.waitForSuccess();
        return instance;
    }

    public void wrapWithTryCatch(String action, ActionFunc actionFunc) throws MMATaskInterruptException {
        try {
            actionFunc.call();
        } catch (Exception e) {
            task.error(action, e);
            throw new MMATaskInterruptException();
        }
    }

    @FunctionalInterface
    public static interface ActionFunc {
        void call() throws Exception;
    }
}
