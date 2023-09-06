package com.aliyun.odps.mma.task;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;


import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.config.HiveOssConfig;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.sql.OdpsSqlUtils;
import com.aliyun.odps.mma.sql.PartitionValue;
import com.aliyun.odps.mma.util.KeyLock;
import com.aliyun.odps.mma.util.OdpsUtils;

/**
 * 为 hive+oss => MC 迁移任务 提供相关 外表操作方法
 */
public class OdpsOssAction extends OdpsAction {
    private final HiveOssConfig ossConfig;
    private final String odpsTempOssTableFullName;
    private final String odpsTempOssTableName;


    public OdpsOssAction(OdpsUtils odpsUtils, TaskProxy task) {
        super(odpsUtils, task);
        this.ossConfig = (HiveOssConfig) task.getJobConfig().getSourceConfig();

        if (task.getOdpsTableName().length() > 120) {
            this.odpsTempOssTableName = "mma_tmp_" + UUID.randomUUID().toString().replaceAll("-", "");
        } else {
            this.odpsTempOssTableName = "mma_tmp_" + task.getOdpsTableName();
        }

        this.odpsTempOssTableFullName = String.format("%s.`%s`", task.getOdpsProjectName(), this.odpsTempOssTableName);
    }


    public void createExternalTable() throws MMATaskInterruptException {
        String location = ossConfig.getTableDataPath(task.getDbName(), task.getTableName());
        TableModel table = task.getTable().getTableModel();
        String serde = table.getSerde();
        Map<String, String> serdeProperties = table.getExtraJson();

        String inputFormat = table.getInputFormat();
        String outputFormat = table.getOutputFormat();
//
//        String storageType = task.getJobConfig().getString(HiveOssConfig.OSS_STORAGE_FILE_TYPE);
//        if (Objects.isNull(storageType)) {
//            String inputFormat = table.getInputFormat();
//            storageType = ossConfig.getOdpsFileFormatByInputFormat(inputFormat);
//        }

        String sql = OdpsSqlUtils.createExternalTableSql(
                odpsTempOssTableFullName,
                task.getOdpsTableSchema(),
                serde,
                serdeProperties,
                inputFormat,
                outputFormat,
                location
        );

        wrapWithTryCatch(
                sql,
                () -> executeSql(sql,  task.getJobConfig().getSourceConfig().getMap(HiveOssConfig.MC_SQL_HINTS))
        );
    }

    public void addPartitionsToExternalTable() throws MMATaskInterruptException {
        if (! task.getTable().isPartitionedTable()) {
            return;
        }

        List<PartitionValue> ptValues = task.getSrcPartitionValues();
        if (ptValues.isEmpty()) {
            return;
        }

        String sql = OdpsSqlUtils.addPartitionsSql(
                odpsTempOssTableFullName,
                ptValues
        );

        wrapWithTryCatch(sql, () -> {
            try (KeyLock keyLock = new KeyLock(odpsTempOssTableFullName)) {
                keyLock.lock();
                executeSql(sql);
            }
        });
    }

    /**
     * 任务结束后清理临时表
     */
    public void dropExternalTable() throws OdpsException {
        odpsUtils.getOdps().tables().delete(task.getOdpsTableName(), odpsTempOssTableName);
    }

    public CompletableFuture<Long> selectCountExternalTable(Consumer<Instance> insGetter) throws MMATaskInterruptException {
        String sql = OdpsSqlUtils.selectCountSql(odpsTempOssTableFullName, task.getSrcPartitionValues());
        return getCountFuture(sql, insGetter, null);
    }

    public void insertOverwriteFromOssToMc(Map<String, String> hints, Consumer<Instance> insGetter) throws MMATaskInterruptException {
        TableSchema tableSchema = task.getOdpsTableSchema();

        String sql = OdpsSqlUtils.insertOverwriteSql(
                odpsTempOssTableFullName,
                task.getOdpsTableFullName(),
                tableSchema,
                task.getSrcPartitionValues()
        );

        wrapWithTryCatch(sql, () -> {
            Instance instance = executeSql(sql, hints);
            insGetter.accept(instance);
        });
    }

}
