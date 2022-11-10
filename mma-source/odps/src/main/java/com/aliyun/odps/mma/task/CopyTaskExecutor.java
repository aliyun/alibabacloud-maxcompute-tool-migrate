package com.aliyun.odps.mma.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.execption.MMATaskInterruptException;
import com.aliyun.odps.mma.service.DbService;
import com.aliyun.odps.mma.sql.OdpsSql;
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


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CopyTaskExecutor extends TaskExecutor {
    Logger logger = LoggerFactory.getLogger(CopyTaskExecutor.class);

    OdpsUtils sourceOdpsUtils;
    OdpsAction sourceOdpsAction;
    Instance dataTransInstance;
    Instance srcCountInstance;
    Instance destCountInstance;
    DbService dbService;

    @Autowired
    public CopyTaskExecutor(DbService dbService) {
        super();
        this.dbService = dbService;
    }

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
    }

    @Override
    public TaskType taskType() {
        return TaskType.ODPS;
    }

    @Override
    protected void _setUpSchema() throws Exception {
        odpsAction.createTableIfNotExists();
    }

    @Override
    protected void _dataTruncate() {}

    @Override
    protected void _dataTrans() throws Exception {
        CopyTask copyTask = createCopyTask();

        try {
            dataTransInstance = sourceOdpsUtils.executeTask(copyTask);
            task.log("execute copytask", sourceOdpsUtils.getLogView(dataTransInstance));
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
        CompletableFuture<Long> destCountFuture = odpsAction.selectCount((ins) -> this.destCountInstance = ins);
        CompletableFuture<Long> sourceCountFuture = sourceOdpsAction.selectCount((ins) -> this.srcCountInstance = ins);
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

    private CopyTask createCopyTask() throws MMATaskInterruptException {
        String sourceProject = task.getDbName();
        String sourceTable = task.getTableName();
        String destProject = task.getOdpsProjectName();
        String destTable = task.getOdpsTableName();
        String destDataEndpoint = mmaConfig.getMcDataEndpoint();
        String destTunnelEndpoint = mmaConfig.getConfig(MMAConfig.MC_TUNNEL_ENDPOINT);

        List<PartitionValue> ptValues = task.getOdpsPartitionValues();
        String partitions = "";

        if (ptValues.size() > 0) { // 大于0的话永远会是1
            partitions = ptValues.get(0).transfer(
                    (name, type, value) -> String.format("%s=%s", name, OdpsSql.adaptOdpsPartitionValue(type, value)),
                    ",");
        }

        Datasource.Direction direction = Datasource.Direction.EXPORT;

        // DEST
        TunnelDatasource tunnel = new TunnelDatasource(direction, destProject, destTable, partitions);
        String token = createToken(direction, destProject, destTable);
        tunnel.setAccountType("token");
        tunnel.setSignature(token);
        tunnel.setOdpsEndPoint(destDataEndpoint);
        if (Objects.nonNull(destTunnelEndpoint)) {
            tunnel.setEndPoint(destTunnelEndpoint);
        }

        // SOURCE
        LocalDatasource local = new LocalDatasource(direction, sourceProject, sourceTable, partitions);

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
        String destAccessId = mmaConfig.getConfig(MMAConfig.MC_AUTH_ACCESS_ID);
        String destAccessKey = mmaConfig.getConfig(MMAConfig.MC_AUTH_ACCESS_KEY);
        int day = 60 * 60 * 24;
        long expires = System.currentTimeMillis() / 1000 + day;
        final String TOKEN_ALGORITHM_ID = "1";
        final String TOKEN_TYPE = "1";
        final String TOKEN_VERSION = "v1";

        String data = TOKEN_ALGORITHM_ID + TOKEN_TYPE + destAccessId + grantee + expires + policy;
        HmacSHA1Signature signer = new HmacSHA1Signature();
        String signature = signer.computeSignature(destAccessKey, data) + "#" + TOKEN_ALGORITHM_ID + "#" + TOKEN_TYPE
                           + "#" + destAccessId + "#" + grantee + "#" + expires + "#" + policy;
        String base64Signature = new String(Base64.encodeBase64(signature.getBytes()));

        return TOKEN_VERSION + "." + base64Signature;
    }
}
