package com.aliyun.odps.mma.task;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.constant.TaskDataStatus;
import com.aliyun.odps.mma.sql.OdpsSqlUtils;
import com.aliyun.odps.mma.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MergedTransportTaskExecutor extends TaskExecutor {
    Logger logger = LoggerFactory.getLogger(MergedTransportTaskExecutor.class);
    protected static String TEMP_TABLE_PREFIX = "TEMP_MMA_MERGED_";
    protected static int DEFAULT_TEMP_TABLE_LIFECYCLE = 15;

    Instance odpsIns;


    @Override
    protected void _dataTrans() throws Exception {
        outer: while (!this.stopped) {
            switch (task.getSubStatus()) {
                case "":
                case "DATA_INIT":
                case "DATA_MERGE_SOURCE_DOING":
                case "DATE_MERGE_SOURCE_FAILED":
                    mergeSourceTable();
                    break;
                case "DATE_MERGE_SOURCE_DONE":
                case "DATA_TRANS_DOING":
                case "DATA_TRANS_FAILED":
                    dataMergedCreateAndTruncateTempTable();
                    dataMergedTrans();
                    break;
                case "DATA_TRANS_DONE":
                case "DATA_UN_MERGE_DST_DOING":
                case "DATA_UN_MERGE_DST_FAILED":
                    if (jobConfig.isNoUnMergePartition()) {
                        break outer;
                    }
                    unMergedDstTable();
                    break;
                case "DATA_UN_MERGE_DST_DONE":
                default:
                    break outer;
            }
        }
    }

    protected void dataMergedCreateAndTruncateTempTable() throws Exception {
        withSubStatus(
                TaskDataStatus.MERGE_SOURCE_DOING,
                TaskDataStatus.MERGE_SOURCE_DONE,
                TaskDataStatus.MERGE_SOURCE_FAILED,
                this::_dataMergedCreateAndTruncateTempTable
        );
    }

    protected void mergeSourceTable() throws Exception {
        withSubStatus(
                TaskDataStatus.MERGE_SOURCE_DOING,
                TaskDataStatus.MERGE_SOURCE_DONE,
                TaskDataStatus.MERGE_SOURCE_FAILED,
                this::_mergeSourceTable
        );
    }

    protected void dataMergedTrans() throws Exception {
        withSubStatus(
                TaskDataStatus.TRANS_DOING,
                TaskDataStatus.TRANS_DONE,
                TaskDataStatus.TRANS_FAILED,
                this::_dataMergedTrans
        );
    }

    protected void unMergedDstTable() throws Exception {
        withSubStatus(
                TaskDataStatus.UN_MERGE_DST_DOING,
                TaskDataStatus.UN_MERGE_DST_DONE,
                TaskDataStatus.UN_MERGE_DST_FAILED,
                this::_unMergedDstTable
        );
    }



    protected void _mergeSourceTable() throws Exception {

    }

    protected void _dataMergedCreateAndTruncateTempTable() throws Exception {
        String createTableSql = createMergedTempTableSql();
        executeOdpsSql(createTableSql);

        String truncatedTableSql = OdpsSqlUtils.truncateTableOrPartitionsSql(
                getTempTableFullName(),
                null
        );

        executeOdpsSql(truncatedTableSql);
    }

    protected void _dataMergedTrans() throws Exception {

    }

    protected void _unMergedDstTable() throws Exception {
        String sql = getUnMergeDstTableSql();
        executeOdpsSql(sql);
    }

    protected String createMergedTempTableSql() {
        TableSchema tableSchema = task.getOdpsTableSchema();
        List<Column> columns = tableSchema.getColumns();
        List<Column> ptColumns = tableSchema.getPartitionColumns();

        TableSchema tempTableSchema = new TableSchema();
        for (Column c: columns) {
            tempTableSchema.addColumn(c);
        }

        for (Column c: ptColumns) {
            tempTableSchema.addColumn(c);
        }

        return OdpsSqlUtils.createTableSql(
                task.getOdpsProjectName(),
                task.getOdpsSchemaName(),
                getTempTableName(),
                tempTableSchema,
                null,
                DEFAULT_TEMP_TABLE_LIFECYCLE
        );
    }


    protected String getUnMergeDstTableSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("insert overwrite table ")
                .append(task.getOdpsTableFullName());

        TableSchema tableSchema = task.getOdpsTableSchema();
        List<String> ptColumns = tableSchema.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toList());

        if (!ptColumns.isEmpty()) {
            String ptColumnsStr = String.join(",", ptColumns);
            sb.append(" partition(")
                    .append(ptColumnsStr)
                    .append(")");
        }

        List<String> columns = tableSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        columns.addAll(ptColumns);

        sb.append("\nselect ")
                .append(String.join(",", columns))
                .append(" from ")
                .append(getTempTableFullName())
                .append(";");


        return sb.toString();
    }


    protected String getTempTableFullName() {
        return String.format("%s.%s%s", task.getOdpsProjectName(), TEMP_TABLE_PREFIX, task.getOdpsTableName());
    }

    protected String getTempTableName() {
        return String.format("%s%s", TEMP_TABLE_PREFIX, task.getOdpsTableName());
    }

    protected void executeOdpsSql(String sql) throws Exception {
        executeOdpsSql(sql, null);
    }

    protected void executeOdpsSql(String sql, Map<String, String> hints) throws Exception {
        odpsAction.wrapWithTryCatch(sql, () -> {
            this.odpsIns = odpsAction.executeSql(sql, hints);
        });
    }

    @Override
    public void killSelf() {
        super.killSelf();

        if (Objects.nonNull(this.odpsIns)) {
            try {
                OdpsUtils.stop(this.odpsIns);
            } catch (Exception _e) {
                // ignore
            }

            odpsIns = null;
        }
    }
}
