package com.aliyun.odps.mma.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.constant.OssTransferState;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.sql.PartitionValue;
import com.aliyun.odps.mma.util.OdpsUtils;
import com.aliyun.odps.mma.util.OssUtils;
import com.aliyun.odps.mma.util.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.aliyun.odps.mma.constant.OssTransferState.DATA_INIT;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OdpsOssTransferTask extends TaskExecutor {
    private OdpsOssAction srcOdpsAction;
    private OdpsOssAction dstOdpsAction;
    private Instance srcOdpsInstance;
    private Instance dstOdpsInstance;
    private OssUtils ossUtils;

    @Override
    public TaskType taskType() {
        return TaskType.ODPS_OSS_TRANSFER;
    }

    @Override
    protected void setUp() {
        OdpsConfig odpsConfig = (OdpsConfig) sourceConfig;
        OdpsUtils srcOdpsUtils = OdpsUtils.fromConfig(sourceConfig);
        srcOdpsAction = new OdpsOssAction(srcOdpsUtils, task, odpsConfig);

        OdpsUtils dstOdpsUtils = OdpsUtils.fromConfig(mmaConfig);
        dstOdpsAction = new OdpsOssAction(dstOdpsUtils, task, odpsConfig);

        ossUtils = new OssUtils(
                odpsConfig.getConfig(OdpsConfig.OSS_ENDPOINT_EXTERNAL),
                odpsConfig.getConfig(OdpsConfig.OSS_AUTH_ACCESS_ID),
                odpsConfig.getConfig(OdpsConfig.OSS_AUTH_ACCESS_KEY)
        );
    }

    @Override
    protected void _setUpSchema() throws Exception {
        dstOdpsAction.createTableIfNotExists();
        if (task.getDstOdpsTableSchema().isDisableLifeCycle()) {
            dstOdpsAction.executeSql("ALTER TABLE " + task.getOdpsTableFullName() + " disable lifecycle;");
        }
        // 向目标表添加分区。虽然insert overwrite会动态创建分区，但存在只迁schema和分区没有数据的情况
        // 这时就需要显示的添加分区
        dstOdpsAction.addPartitions();
        dstOdpsAction.disableLifeCycle();
    }

    @Override
    protected void _dataTruncate() throws Exception {
        task.setSubStatus(DATA_INIT.name());
        dstOdpsAction.truncate();
    }

    @Override
    protected void _dataTrans() throws Exception {
        outer: while (!this.stopped.get()) {
            switch (OssTransferState.fromString(task.getSubStatus())) {
                case DATA_INIT:
                case SRC_CREATE_EXTERNAL_TABLE_DOING:
                case SRC_CREATE_EXTERNAL_TABLE_FAILED:
                    // 建源OSS外表
                    withSubStatus(
                            OssTransferState.SRC_CREATE_EXTERNAL_TABLE_DOING,
                            OssTransferState.SRC_CREATE_EXTERNAL_TABLE_DONE,
                            OssTransferState.SRC_CREATE_EXTERNAL_TABLE_FAILED,
                            () -> srcOdpsAction.createSrcExternalTable()
                    );
                    break;
                case SRC_CREATE_EXTERNAL_TABLE_DONE:
                case SRC_WRITE_EXTERNAL_TABLE_DOING:
                case SRC_WRITE_EXTERNAL_TABLE_FAILED:
                    // 源表数据写源OSS外表, 这时会动态创建OSS外表的分区
                    withSubStatus(
                            OssTransferState.SRC_WRITE_EXTERNAL_TABLE_DOING,
                            OssTransferState.SRC_WRITE_EXTERNAL_TABLE_DONE,
                            OssTransferState.SRC_WRITE_EXTERNAL_TABLE_FAILED,
                            () -> srcOdpsAction.insertOverWriteSrcExternalFromSrc((ins) -> this.srcOdpsInstance = ins)
                    );
                    break;
                case SRC_WRITE_EXTERNAL_TABLE_DONE:
                case DST_CREATE_EXTERNAL_TABLE_DOING:
                case DST_CREATE_EXTERNAL_TABLE_FAILED:
                    // 创建目标OSS外表
                    withSubStatus(
                            OssTransferState.DST_CREATE_EXTERNAL_TABLE_DOING,
                            OssTransferState.DST_CREATE_EXTERNAL_TABLE_DONE,
                            OssTransferState.DST_CREATE_EXTERNAL_TABLE_FAILED,
                            () -> dstOdpsAction.createDstExternalTable()
                    );
                    break;
                case DST_CREATE_EXTERNAL_TABLE_DONE:
                case DST_ADD_EXTERNAL_PTS_DOING:
                case DST_ADD_EXTERNAL_PTS_FAILED:
                    // 添加目标OSS外表的分区
                    withSubStatus(
                            OssTransferState.DST_ADD_EXTERNAL_PTS_DOING,
                            OssTransferState.DST_ADD_EXTERNAL_PTS_DONE,
                            OssTransferState.DST_ADD_EXTERNAL_PTS_FAILED,
                            () -> dstOdpsAction.addDstExternalTablePartitions()
                    );
                    break;
                case DST_ADD_EXTERNAL_PTS_DONE:
                case DST_EXTERNAL_TABLE_TO_INNER_TABLE_DOING:
                case DST_EXTERNAL_TABLE_TO_INNER_TABLE_FAILED:
                    // 目标外表转目标内表
                    withSubStatus(
                            OssTransferState.DST_EXTERNAL_TABLE_TO_INNER_TABLE_DOING,
                            OssTransferState.DST_EXTERNAL_TABLE_TO_INNER_TABLE_DONE,
                            OssTransferState.DST_EXTERNAL_TABLE_TO_INNER_TABLE_FAILED,
                            () -> dstOdpsAction.insertOverWriteDstFromExternalDst((inst -> this.dstOdpsInstance = inst))
                    );
                    break;
                case DST_EXTERNAL_TABLE_TO_INNER_TABLE_DONE:
                case SRC_DELETE_EXTERNAL_PTS_DOING:
                case SRC_DELETE_EXTERNAL_PTS_FAILED:
                    // 删除源OSS外表分区
                    withSubStatus(
                            OssTransferState.SRC_DELETE_EXTERNAL_PTS_DOING,
                            OssTransferState.SRC_DELETE_EXTERNAL_PTS_DONG,
                            OssTransferState.SRC_DELETE_EXTERNAL_PTS_FAILED,
                            () -> srcOdpsAction.dropSrcExternalPts()
                    );
                    break;
                case SRC_DELETE_EXTERNAL_PTS_DONG:
                case DST_DELETE_EXTERNAL_PTS_DOING:
                case DST_DELETE_EXTERNAL_PTS_FAILED:
                    // 删除源OSS外表分区，外表要加lifecycle
                    withSubStatus(
                            OssTransferState.DST_DELETE_EXTERNAL_PTS_DOING,
                            OssTransferState.DST_DELETE_EXTERNAL_PTS_DONE,
                            OssTransferState.DST_DELETE_EXTERNAL_PTS_FAILED,
                            () -> dstOdpsAction.dropDstExternalPts()
                    );
                    break;
                case DST_DELETE_EXTERNAL_PTS_DONE:
                case OSS_DELETE_OBJECT_DOING:
                case OSS_DELETE_OBJECT_FAILED:
                    task.log("delete oss object", "start to delete oss object");
                    // 删除OSS数据
                    withSubStatus(
                            OssTransferState.OSS_DELETE_OBJECT_DOING,
                            OssTransferState.OSS_DELETE_OBJECT_DONE,
                            OssTransferState.OSS_DELETE_OBJECT_FAILED,
                            () -> {
                                OdpsConfig odpsConfig = (OdpsConfig) sourceConfig;
                                String bucket = odpsConfig.getConfig(OdpsConfig.OSS_BUCKET);
                                List<PartitionValue> pts =  task.getSrcPartitionValues();

                                StringBuilder objectPathSb = new StringBuilder(task.getDbName());
                                if (StringUtils.isBlank(task.getSchemaName())) {
                                    objectPathSb.append("/").append(task.getTableName());
                                } else {
                                    objectPathSb.append("/").append(task.getSchemaName())
                                            .append("/").append(task.getTableName());
                                }

                                final String objectPath = objectPathSb.toString();

                                try {
                                    if (! task.getTable().isPartitionedTable()) {
                                        ossUtils.deleteDir(bucket, objectPath);
                                    }

                                    if (! pts.isEmpty()) {
                                        List<String> ptPathList = pts.stream()
                                                .map(pv -> objectPath + "/" + pv.transfer((name, type, value) -> {
                                                    return String.format("%s=%s", name, value);
                                                }, "/"))
                                                .collect(Collectors.toList());

                                        ossUtils.deleteDir(bucket, objectPath, ptPathList);
                                    }
                                } catch (Exception e) {
                                    task.log("delete oss object", "failed to delete oss object");
                                    throw e;
                                }

                            }
                    );
                    task.log("delete oss object", "success to delete oss object");
                case OSS_DELETE_OBJECT_DONE:
                default:
                    break outer;
            }
        }
    }

    @Override
    protected void _verifyData() throws Exception {
        CompletableFuture<Map<String, Long>> destCountFuture = dstOdpsAction.selectCountByPt(
                task.getOdpsTableFullName(),
                task.getDstOdpsPartitionValues(),
                (ins) -> this.dstOdpsInstance = ins,
                sourceConfig.getMap(OdpsConfig.MC_SQL_HINTS)
        );

        CompletableFuture<Map<String, Long>> sourceCountFuture = srcOdpsAction.selectCountByPt(
                task.getTableFullName(),
                task.getSrcPartitionValues(),
                (ins) -> this.srcOdpsInstance = ins,
                sourceConfig.getMap(OdpsConfig.MC_SQL_HINTS)
        );
        sourceCountFuture.join();

        VerificationAction.countByPtResultCompare(
                "src odps", sourceCountFuture.get(),
                "dest odps", destCountFuture.get(),
                task
        );
    }

    @Override
    public void killSelf() {
        super.killSelf();

        OdpsUtils.stop(srcOdpsInstance);
        srcOdpsAction = null;
        OdpsUtils.stop(dstOdpsInstance);
        dstOdpsAction = null;
    }
}
