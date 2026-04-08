package com.aliyun.odps.mma.task;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.orm.TableProxy;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.sql.OdpsSqlUtils;
import com.aliyun.odps.mma.sql.PartitionValue;
import com.aliyun.odps.mma.util.KeyLock;
import com.aliyun.odps.mma.util.OdpsUtils;
import com.aliyun.odps.mma.util.StringUtils;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class OdpsOssAction extends OdpsAction {
    private final OdpsConfig odpsConfig;

    public OdpsOssAction(OdpsUtils odpsUtils, TaskProxy task, OdpsConfig odpsConfig) {
        super(odpsUtils, task);
        this.odpsConfig = odpsConfig;
    }

    public String getOdpsSrcOssTableFullName() {
        String tableName =  "mma_src_external_temp_" + task.getTableName();
        if (task.getTableName().length() > 120) {
            tableName = "mma_src_external_temp_" + task.getId();
        }

        if (StringUtils.isBlank(task.getSchemaName())) {
            return String.format("%s.%s", task.getDbName(), tableName);
        }

        return String.format("%s.%s.%s", task.getDbName(), task.getSchemaName(), tableName);
    }

    public String getOdpsDstOssTableFullName() {
        String tableName = "mma_dst_external_temp_" + task.getOdpsTableName();
        if (task.getOdpsTableName().length() > 120) {
            tableName = "mma_dst_external_temp_" + task.getId();
        }

        if (StringUtils.isBlank(task.getOdpsSchemaName())) {
            return String.format("%s.%s", task.getOdpsProjectName(), tableName);
        }

        return String.format("%s.%s.%s", task.getOdpsProjectName(), task.getOdpsSchemaName(), tableName);
    }

    public void createSrcExternalTable() throws MMATaskInterruptException {
        TableProxy table = task.getTable();

        createExternalTable(
                getOdpsSrcOssTableFullName(),
                odpsConfig.getOssPathOfExternalTable(
                        table.getDbName(),
                        table.getSchemaName(),
                        table.getName()
                )
        );
    }

    public void createDstExternalTable() throws MMATaskInterruptException {
        TableProxy table = task.getTable();

        createExternalTable(
                getOdpsDstOssTableFullName(),
                odpsConfig.getOssPathOfExternalTable(
                        table.getDbName(),
                        table.getSchemaName(),
                        table.getName()
                )
        );
    }

    private void createExternalTable(String externalTableName, String ossLocation) throws MMATaskInterruptException {

        String serde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
        String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
        String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";

//        String serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
//        String inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
//        String outputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";

        Map<String, String> serdeProperties = new HashMap<>();
        serdeProperties.put("mcfed.orc.compress", "ZLIB");

        // orc不支持datetime，需要将datetime类型数据转换为timestamp类型
        TableSchema newTs = new TableSchema();
        List<Column> columns = task.getTableSchemaOfOdpsSrc().getColumns();
        List<Column> ptColumns = task.getTableSchemaOfOdpsSrc().getPartitionColumns();

        for (Column column : columns) {
            if (column.getTypeInfo().getOdpsType() == OdpsType.DATETIME) {
                Column newColumn = new Column(
                        column.getName(),
                        TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP),
                        column.getComment()
                );

                newColumn.setDefaultValue(column.getDefaultValue());
                newTs.addColumn(newColumn);
                continue;
            }

            if (column.getTypeInfo().getOdpsType() == OdpsType.DECIMAL) {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) column.getTypeInfo();
                int precision = decimalTypeInfo.getPrecision();

                if (precision > 38) {
                    Column newColumn = new Column(
                            column.getName(),
                            TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING),
                            column.getComment()
                    );

                    newColumn.setDefaultValue(column.getDefaultValue());
                    newTs.addColumn(newColumn);
                    continue;
                }
            }

            newTs.addColumn(column);
        }

        for (Column column : ptColumns) {
            newTs.addPartitionColumn(column);
        }

        String sql = OdpsSqlUtils.createExternalTableSql(
                externalTableName,
                newTs,
                serde,
                serdeProperties,
                inputFormat,
                outputFormat,
                ossLocation,
                1
        );

        wrapWithTryCatch(
                sql,
                () -> executeSql(sql,  odpsConfig.getMap(OdpsConfig.MC_SQL_HINTS))
        );
    }

    public void insertOverWriteSrcExternalFromSrc(Consumer<Instance> insGetter) throws MMATaskInterruptException {
        TableSchema schema = task.getTableSchemaOfOdpsSrc();

        String sql = OdpsSqlUtils.insertOverwriteSql(
                task.getTableFullName(),
                getOdpsSrcOssTableFullName(),
                schema,
                task.getSrcPartitionValues()
        );

        Map<String, String> hints = odpsConfig.getMap(OdpsConfig.MC_SQL_HINTS);
        hints.put("odps.sql.unstructured.oss.commit.mode", "true");

        wrapWithTryCatch(sql, () -> {
            Instance instance = executeSqlNoWait(sql, hints);
            insGetter.accept(instance);
            instance.waitForSuccess();
        });
    }

    public void insertOverWriteDstFromExternalDst(Consumer<Instance> insGetter) throws MMATaskInterruptException {
        TableSchema schema = task.getDstOdpsTableSchema();

        String sql = OdpsSqlUtils.insertOverwriteSql(
                getOdpsDstOssTableFullName(),
                task.getOdpsTableFullName(),
                schema,
                task.getDstOdpsPartitionValues()
        );

        Map<String, String> hints = odpsConfig.getMap(OdpsConfig.MC_SQL_HINTS);

        wrapWithTryCatch(sql, () -> {
            Instance instance = executeSqlNoWait(sql, hints);
            insGetter.accept(instance);
            instance.waitForSuccess();
        });
    }

    public void addDstExternalTablePartitions() throws MMATaskInterruptException {
        if (! task.getTable().isPartitionedTable()) {
            return;
        }

        List<PartitionValue> ptValues = task.getDstOdpsPartitionValues();
        if (ptValues.isEmpty()) {
            return;
        }

        String dstOssTableFullName = getOdpsDstOssTableFullName();

        String sql = OdpsSqlUtils.addPartitionsSql(
                dstOssTableFullName,
                ptValues
        );

        wrapWithTryCatch(sql, () -> {
            try (KeyLock keyLock = new KeyLock(dstOssTableFullName)) {
                keyLock.lock();
                executeSql(sql);
            }
        });
    }

    public void dropSrcExternalPts() throws MMATaskInterruptException {
        dropExternalPts(getOdpsSrcOssTableFullName());
    }

    public void dropDstExternalPts() throws MMATaskInterruptException {
        dropExternalPts(getOdpsDstOssTableFullName());
    }

    private void dropExternalPts(String tableFullName) throws MMATaskInterruptException {
        if (! task.getTable().isPartitionedTable()) {
            return;
        }

        List<PartitionValue> ptValues = task.getSrcPartitionValues();
        if (ptValues.isEmpty()) {
            return;
        }

        String sql = OdpsSqlUtils.dropPartitions(
                tableFullName,
                ptValues
        );

        wrapWithTryCatch(sql, () -> {
            try (KeyLock keyLock = new KeyLock(tableFullName)) {
                keyLock.lock();
                executeSql(sql);
            }
        });
    }
}
