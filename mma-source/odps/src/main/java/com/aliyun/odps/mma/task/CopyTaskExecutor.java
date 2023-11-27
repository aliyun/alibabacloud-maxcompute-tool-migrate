package com.aliyun.odps.mma.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.service.DbService;
import com.aliyun.odps.mma.sql.OdpsSqlUtils;
import com.aliyun.odps.mma.sql.PartitionValue;
import com.aliyun.odps.mma.util.OdpsUtils;
import com.aliyun.odps.task.CopyTask;
import com.aliyun.odps.task.copy.Datasource;
import com.aliyun.odps.task.copy.LocalDatasource;
import com.aliyun.odps.task.copy.TunnelDatasource;
import com.aliyun.oss.common.auth.HmacSHA1Signature;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CopyTaskExecutor extends TaskExecutor {
    Logger logger = LoggerFactory.getLogger(CopyTaskExecutor.class);

    OdpsUtils sourceOdpsUtils;
    OdpsAction sourceOdpsAction;
    Instance dataTransInstance;
    Instance srcCountInstance;
    Instance destCountInstance;
    Datasource.Direction copyTaskDirection;
    OdpsUtils odpsUtils;


    @Override
    protected void setUp() {
        sourceOdpsUtils = OdpsUtils.fromConfig(sourceConfig);
        sourceOdpsAction = new OdpsAction(
                sourceOdpsUtils,
                task,
                task.getDbName(),
                task.getTableName(),
                task.getTableFullName()
        );
        String directionStr = sourceConfig.getConfig(OdpsConfig.COPYTASK_DIRECTION);
        copyTaskDirection = Datasource.Direction.valueOf(directionStr);
        odpsUtils = OdpsUtils.fromConfig(mmaConfig);
    }

    @Override
    public TaskType taskType() {
        return TaskType.ODPS;
    }

    @Override
    protected void _setUpSchema() throws Exception {
        odpsAction.createTableIfNotExists();

        if (copyTaskDirection == Datasource.Direction.IMPORT) {
            odpsAction.addPartitions();
        }
    }

    @Override
    protected void _dataTruncate() throws Exception {
        if (copyTaskDirection == Datasource.Direction.IMPORT) {
            odpsAction.truncate();
        }
    }

    @Override
    protected void _dataTrans() throws Exception {
        CopyTask copyTask = createCopyTask();

        try {
            OdpsUtils odpsUtils;

            if (Datasource.Direction.EXPORT.equals(copyTaskDirection)) {
                odpsUtils = sourceOdpsUtils;
            } else {
                odpsUtils = this.odpsUtils;
            }

            dataTransInstance = odpsUtils.executeTask(copyTask);
            task.log("execute copytask", odpsUtils.getLogView(dataTransInstance));
            dataTransInstance.waitForSuccess();
        } catch (OdpsException e) {
            if (!this.stopped) {
                task.error("copytask failed", e);
                logger.warn("copytask failed for {}", task.getTaskName(), e);
            }

            throw new MMATaskInterruptException();
        }
    }

    @Override
    protected void _verifyData() throws Exception {
        CompletableFuture<Long> destCountFuture = odpsAction.selectSrcCount(
                task.getOdpsTableFullName(),
                (ins) -> this.destCountInstance = ins
        );
        CompletableFuture<Long> sourceCountFuture = sourceOdpsAction.selectDstCount(
                task.getTableFullName(),
                (ins) -> this.srcCountInstance = ins
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
        dataTransInstance = null;
        OdpsUtils.stop(srcCountInstance);
        srcCountInstance = null;
        OdpsUtils.stop(destCountInstance);
        destCountInstance = null;
    }

    private CopyTask createCopyTask() throws Exception {
        String sourceProject = task.getDbName();
        String sourceTable = task.getTableName();
        String destProject = task.getOdpsProjectName();
        String destTable = task.getOdpsTableName();

        List<PartitionValue> srcPtValues = task.getSrcPartitionValues();
        String srcPartitions = "";

        if (!srcPtValues.isEmpty()) { // 大于0的话永远会是1
            srcPartitions = srcPtValues.get(0).transfer(
                    (name, type, value) -> String.format("%s=%s", name, OdpsSqlUtils.adaptOdpsPartitionValue(type, value)),
                    ",");
        }

        List<PartitionValue> dstPtValues = task.getDstOdpsPartitionValues();
        String dstPartitions = "";

        if (!dstPtValues.isEmpty()) {
            dstPartitions = dstPtValues.get(0).transfer(
                    (name, type, value) -> String.format("%s=%s", name, OdpsSqlUtils.adaptOdpsPartitionValue(type, value)),
                    ","
            );
        }

        LocalDatasource local;
        TunnelDatasource tunnel;

        if (copyTaskDirection == Datasource.Direction.EXPORT) {
            // LOCAL, 运行copy task的地方，export模式下为源端
            local = new LocalDatasource(copyTaskDirection, sourceProject, sourceTable, srcPartitions);

            // 使用目的端的tunnel
            tunnel = new TunnelDatasource(copyTaskDirection, destProject, destTable, dstPartitions);
            String token = createToken(copyTaskDirection, destProject, destTable);
            tunnel.setAccountType("token");
            tunnel.setSignature(token);

            String destDataEndpoint = mmaConfig.getMcDataEndpoint();
            String destTunnelEndpoint = mmaConfig.getConfig(MMAConfig.MC_TUNNEL_ENDPOINT);

            tunnel.setOdpsEndPoint(destDataEndpoint);
            if (Objects.nonNull(destTunnelEndpoint)) {
                tunnel.setEndPoint(destTunnelEndpoint);
            }
        } else {
            // LOCAL, 运行copy task的地方，import模式下为目的端
            local = new LocalDatasource(copyTaskDirection, destProject, destTable, dstPartitions);

            // 使用源端的tunnel
            tunnel = new TunnelDatasource(copyTaskDirection, sourceProject, sourceTable, srcPartitions);
            String token = createToken(copyTaskDirection, sourceProject, sourceTable);
            tunnel.setAccountType("token");
            tunnel.setSignature(token);

            String srcOdpsEndpoint = sourceConfig.getConfig(OdpsConfig.MC_DATA_ENDPOINT);
            String srcTunnelEndpoint = sourceConfig.getConfig(OdpsConfig.MC_TUNNEL_ENDPOINT);
            tunnel.setOdpsEndPoint(srcOdpsEndpoint);

            if (Objects.nonNull(srcTunnelEndpoint)) {
                tunnel.setEndPoint(srcTunnelEndpoint);
            }

            String[] chinaRegions = new String[] {
                    "hangzhou",
                    "shanghai",
                    "beijing",
                    "cn-north-2-gov",
                    "zhangjiakou",
                    "shenzhen",
                    "chengdu",
            };

            Predicate<String> isChinaVpc = (url) -> {
                if (!url.contains("-inc")) {
                    return false;
                }

                for (String region: chinaRegions) {
                    if (url.contains(region)) {
                        return true;
                    }
                }

                return false;
            };

            String destDataEndpoint = mmaConfig.getMcDataEndpoint();
            String destTunnelEndpoint = mmaConfig.getConfig(MMAConfig.MC_TUNNEL_ENDPOINT);

            if (Objects.nonNull(destTunnelEndpoint)) {
                if (Objects.nonNull(srcTunnelEndpoint)) {
                    if ((! isChinaVpc.test(destTunnelEndpoint)) && isChinaVpc.test(sourceProject)) {
                        throw new Exception("cannot download data");
                    }
                } else {
                    if ((! isChinaVpc.test(destTunnelEndpoint)) && isChinaVpc.test(srcOdpsEndpoint)) {
                        throw new Exception("cannot download data");
                    }
                }
            } else {
                if (Objects.nonNull(srcTunnelEndpoint)) {
                    if ((! isChinaVpc.test(destDataEndpoint)) && isChinaVpc.test(sourceProject)) {
                        throw new Exception("cannot download data");
                    }
                } else {
                    if ((! isChinaVpc.test(destDataEndpoint)) && isChinaVpc.test(srcOdpsEndpoint)) {
                        throw new Exception("cannot download data");
                    }
                }
            }
        }

        // SETUP Task
        CopyTask copyTask = new CopyTask("copy_task_" + Calendar.getInstance().getTimeInMillis());

        //  Append(-a)和Overwrite(-o)的语义很明确
        //  不过tunnel其实是只支持append操作
        //  所以overwrite模式只不过是帮你执行了一下alter table drop partition和add partition的操作。
        copyTask.setMode("Overwrite");
        copyTask.setLocalInfo(local);
        copyTask.SetTunnelInfo(tunnel);

        String insNum = jobConfig.getString(OdpsConfig.COPYTASK_INS_NUM);
        copyTask.setJobInstanceNumber(insNum);

        JsonObject params = new JsonObject();
        // compatible: 是否兼容odps新的数据类型，如，struct/array等
        final String ODPS_COPY_COMPATIBLE_ENABLED = "odps.copy.compatible.enabled";
        params.addProperty(ODPS_COPY_COMPATIBLE_ENABLED, "false");
        // params.addProperty(ODPS_COPY_STRIP_ENABLED, "false");
        copyTask.setProperty("settings", params.toString());

        return copyTask;
    }

    private String createToken(Datasource.Direction direction, String project, String table)
        throws MMATaskInterruptException {

        // 1. setup policy
        String policy;
        String tableUrl = "projects/" + project + "/" + "tables/" + table;
        String projectUrl = "projects/" + project;
        String instanceUrl = "projects/" + project + "/instances/*";
        switch (direction.toString()) {
            case "EXPORT":
                policy = "{\"Version\":\"1\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"odps:Describe\",\"odps:Update\",\"odps:Alter\",\"odps:Drop\",\"odps:Create\"],\"Resource\":\"acs:odps:*:"
                         + tableUrl.toLowerCase()
                         + "\"},{\"Effect\":\"Allow\",\"Action\":[\"odps:CreateInstance\"],\"Resource\":\"acs:odps:*:"
                         + projectUrl.toLowerCase()
                         + "\"},{\"Effect\":\"Allow\",\"Action\":[\"odps:Read\"],\"Resource\":\"acs:odps:*:"
                         + instanceUrl.toLowerCase() + "\"}]}";
                break;
            case "IMPORT":
                policy = "{\"Version\":\"1\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"odps:Describe\",\"odps:Select\",\"odps:Download\"],\"Resource\":\"acs:odps:*:"
                         + tableUrl.toLowerCase() + "\"}]}";
                break;
            default:
                task.error("copy task create token", "Unknown Direction mode: " + direction);
                throw new MMATaskInterruptException();
        }

        // 2. create token
        String grantee = "";
        String accessId;
        String accessKey;

        if (direction == Datasource.Direction.EXPORT) {
            accessId = mmaConfig.getConfig(MMAConfig.MC_AUTH_ACCESS_ID);
            accessKey = mmaConfig.getConfig(MMAConfig.MC_AUTH_ACCESS_KEY);
        } else {
            accessId = sourceConfig.getConfig(OdpsConfig.MC_AUTH_ACCESS_ID);
            accessKey = sourceConfig.getConfig(OdpsConfig.MC_AUTH_ACCESS_KEY);
        }

        int day = 60 * 60 * 24;
        long expires = System.currentTimeMillis() / 1000 + day;
        final String TOKEN_ALGORITHM_ID = "1";
        final String TOKEN_TYPE = "1";
        final String TOKEN_VERSION = "v1";

        String data = TOKEN_ALGORITHM_ID + TOKEN_TYPE + accessId + grantee + expires + policy;
        HmacSHA1Signature signer = new HmacSHA1Signature();
        String signature = signer.computeSignature(accessKey, data) + "#" + TOKEN_ALGORITHM_ID + "#" + TOKEN_TYPE
                           + "#" + accessId + "#" + grantee + "#" + expires + "#" + policy;
        String base64Signature = new String(Base64.encodeBase64(signature.getBytes()));

        return TOKEN_VERSION + "." + base64Signature;
    }
}
